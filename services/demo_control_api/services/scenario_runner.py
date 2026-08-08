"""Managed in-process execution through the existing generator interfaces."""

import asyncio
import random
from contextlib import suppress
from time import perf_counter
from uuid import UUID

from services.demo_control_api.config import DemoConfig
from services.demo_control_api.metrics import (
    GENERATOR_EVENTS_GENERATED,
    GENERATOR_GENERATION_DURATION,
    GENERATOR_INTER_EVENT_INTERVAL,
    GENERATOR_MISSED_DEADLINES,
    GENERATOR_SCHEDULER_DRIFT,
    GENERATOR_STAGE_DURATION,
)
from services.demo_control_api.models.runs import TERMINAL_STATUSES, DemoRun, RunStatus
from services.demo_control_api.models.scenarios import ScenarioType
from services.demo_control_api.repositories.demo_runs import DemoRunRepository
from services.event_generator.anomalies import AnomalyInjector
from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.producer import KafkaEventProducer
from shared.commerce_common.enums import CustomerPersona

PERSONA_MAP = {
    ScenarioType.NORMAL: CustomerPersona.NORMAL,
    ScenarioType.SUSPICIOUS: CustomerPersona.SUSPICIOUS,
    ScenarioType.TAKEOVER: CustomerPersona.ACCOUNT_TAKEOVER,
    ScenarioType.BOT: CustomerPersona.BOT,
    ScenarioType.REFUND: CustomerPersona.SUSPICIOUS,
    ScenarioType.DUPLICATE: CustomerPersona.NORMAL,
    ScenarioType.MALFORMED: CustomerPersona.NORMAL,
}

PROGRESS_REFRESH_INTERVAL_SECONDS = 0.5


class ConcurrencyLimitError(RuntimeError):
    pass


class ScenarioRunner:
    def __init__(self, config: DemoConfig, repository: DemoRunRepository) -> None:
        self.config = config
        self.repository = repository
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def start(self, run: DemoRun) -> DemoRun:
        async with self._lock:
            if run.run_id in self._tasks:
                return self.repository.get(run.run_id) or run
            if run.status is not RunStatus.PENDING:
                return run
            active = sum(not task.done() for task in self._tasks.values())
            if active >= self.config.demo_max_concurrent_runs:
                raise ConcurrencyLimitError("maximum concurrent demo runs reached")
            self.repository.transition(run.run_id, RunStatus.STARTING)
            self._tasks[run.run_id] = asyncio.create_task(self._execute(run.run_id))
        return self.repository.get(run.run_id) or run

    async def stop(self, run: DemoRun) -> DemoRun:
        if run.status in TERMINAL_STATUSES:
            return run
        if run.status is RunStatus.RUNNING:
            self.repository.transition(run.run_id, RunStatus.STOP_REQUESTED)
        task = self._tasks.get(run.run_id)
        if task is not None:
            task.cancel()
        return self.repository.get(run.run_id) or run

    async def _execute(self, run_id: UUID) -> None:
        run = self.repository.get(run_id)
        if run is None:
            return
        try:
            self.repository.transition(run_id, RunStatus.RUNNING)
            await asyncio.wait_for(
                self._generate(run),
                timeout=self.config.demo_run_timeout_seconds,
            )
            if run.scenario_type not in {
                ScenarioType.MALFORMED,
                ScenarioType.DUPLICATE,
            }:
                await self._wait_for_processing(run)
            current = self.repository.get(run_id)
            if current and current.status is RunStatus.RUNNING:
                self.repository.refresh(run_id)
                self.repository.transition(
                    run_id, RunStatus.COMPLETED, message="Scenario generation completed"
                )
        except asyncio.CancelledError:
            current = self.repository.get(run_id)
            if current and current.status is RunStatus.STOP_REQUESTED:
                self.repository.transition(
                    run_id, RunStatus.STOPPED, message="Stopped by user"
                )
        except TimeoutError:
            self.repository.transition(
                run_id,
                RunStatus.FAILED,
                message="Run timed out",
                error_category="timeout",
            )
        except Exception:
            current = self.repository.get(run_id)
            if current and current.status not in TERMINAL_STATUSES:
                self.repository.transition(
                    run_id,
                    RunStatus.FAILED,
                    message="Scenario execution failed",
                    error_category="scenario_execution",
                )
        finally:
            self._tasks.pop(run_id, None)

    async def _generate(self, run: DemoRun) -> None:
        generation_started = perf_counter()
        request = run.parameters
        persona = PERSONA_MAP.get(request.scenario_type)
        weights = request.persona_distribution
        weight_string = None
        if weights:
            weight_string = ",".join(
                f"{name}={value}" for name, value in weights.items()
            )
        config = GeneratorConfig(
            kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
            generator_seed=request.seed,
            generator_persona=persona,
            generator_persona_weights=weight_string
            or GeneratorConfig().generator_persona_weights,
            generator_add_to_cart_probability=1 if request.transaction_enabled else 0,
            generator_checkout_probability=1 if request.transaction_enabled else 0,
            generator_refund_probability=1
            if request.scenario_type is ScenarioType.REFUND
            else 0,
            generator_anomalies_enabled=request.scenario_type
            in {ScenarioType.DUPLICATE, ScenarioType.MALFORMED, ScenarioType.MIXED},
            generator_duplicate_event_probability=1
            if request.scenario_type is ScenarioType.DUPLICATE
            else request.duplicate_rate,
            generator_malformed_json_probability=1
            if request.scenario_type is ScenarioType.MALFORMED
            and request.malformed_case == "malformed_json"
            else request.malformed_rate,
            generator_missing_field_probability=1
            if request.scenario_type is ScenarioType.MALFORMED
            and request.malformed_case == "missing_field"
            else 0,
            generator_unknown_event_type_probability=1
            if request.scenario_type is ScenarioType.MALFORMED
            and request.malformed_case == "unknown_event_type"
            else 0,
            generator_payload_mismatch_probability=1
            if request.scenario_type is ScenarioType.MALFORMED
            and request.malformed_case == "payload_mismatch"
            else 0,
            generator_max_anomalies_per_journey=2,
        )
        rng = random.Random(request.seed)
        synthetic = SyntheticGenerator(rng, SeededUuidFactory(request.seed))
        builder = JourneyBuilder(config, synthetic, SystemClock())
        producer = KafkaEventProducer(config)
        injector = AnomalyInjector(config, rng)
        generated = 0
        interval = 1 / request.events_per_second
        loop = asyncio.get_running_loop()
        next_deadline = loop.time()
        previous_event_started: float | None = None
        latest_generated = 0
        progress_changed = asyncio.Event()
        stop_progress = asyncio.Event()

        async def progress_updater() -> None:
            refreshed_generated = 0
            while True:
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        progress_changed.wait(),
                        timeout=PROGRESS_REFRESH_INTERVAL_SECONDS,
                    )
                progress_changed.clear()

                target_generated = latest_generated
                if target_generated != refreshed_generated:
                    refresh_started = perf_counter()
                    await asyncio.to_thread(
                        self.repository.refresh, run.run_id, target_generated
                    )
                    GENERATOR_STAGE_DURATION.labels("progress_refresh").observe(
                        perf_counter() - refresh_started
                    )
                    refreshed_generated = target_generated

                if stop_progress.is_set():
                    return

        progress_task = asyncio.create_task(progress_updater())
        try:
            generation_loop_started = perf_counter()
            while generated < request.event_count:
                build_started = perf_counter()
                journey = builder.build()
                GENERATOR_STAGE_DURATION.labels("journey_build").observe(
                    perf_counter() - build_started
                )
                anomaly_started = perf_counter()
                messages = injector.prepare(journey.events)
                GENERATOR_STAGE_DURATION.labels("anomaly_prepare").observe(
                    perf_counter() - anomaly_started
                )
                remaining = request.event_count - generated
                messages = messages[:remaining]
                manifest_started = perf_counter()
                self.repository.add_manifest(
                    run.run_id,
                    [
                        (message.event_id, message.event_type)
                        for message in messages
                        if message.event_id
                    ],
                )
                GENERATOR_STAGE_DURATION.labels("add_manifest").observe(
                    perf_counter() - manifest_started
                )
                for message in messages:
                    event_started = loop.time()
                    if previous_event_started is not None:
                        GENERATOR_INTER_EVENT_INTERVAL.observe(
                            event_started - previous_event_started
                        )
                    previous_event_started = event_started
                    publish_started = perf_counter()
                    producer.publish_message(message)
                    GENERATOR_STAGE_DURATION.labels("publish_message").observe(
                        perf_counter() - publish_started
                    )
                    generated += 1
                    latest_generated = generated
                    GENERATOR_EVENTS_GENERATED.labels(request.scenario_type.value).inc()
                    progress_changed.set()
                    if interval:
                        next_deadline += interval
                        now = loop.time()
                        lateness = now - next_deadline
                        if lateness > 0:
                            GENERATOR_SCHEDULER_DRIFT.observe(lateness)
                            GENERATOR_MISSED_DEADLINES.labels(
                                request.scenario_type.value
                            ).inc()
                            if lateness > interval:
                                # Rebase after a long miss so delayed work
                                # cannot create an unbounded catch-up burst.
                                next_deadline = now
                        if now < next_deadline:
                            delay = next_deadline - now
                            pacing_started = perf_counter()
                            await asyncio.sleep(delay)
                            GENERATOR_STAGE_DURATION.labels("pacing_sleep").observe(
                                perf_counter() - pacing_started
                            )
                producer.poll(0)
            GENERATOR_GENERATION_DURATION.labels("generation_loop").observe(
                perf_counter() - generation_loop_started
            )
            flush_started = perf_counter()
            producer.flush()
            GENERATOR_STAGE_DURATION.labels("kafka_flush").observe(
                perf_counter() - flush_started
            )

            # Signal shutdown only after generation and producer delivery have
            # completed. The updater performs and awaits the final refresh
            # before exiting, so generated_event_count is durable on return.
            stop_progress.set()
            progress_changed.set()
            await progress_task
            GENERATOR_GENERATION_DURATION.labels("generation_total").observe(
                perf_counter() - generation_started
            )
        except BaseException:
            if not progress_task.done():
                progress_task.cancel()
            with suppress(asyncio.CancelledError):
                await progress_task
            raise

    async def _wait_for_processing(self, run: DemoRun) -> None:
        """Bound completion waiting without arbitrary sleeps."""
        deadline = asyncio.get_running_loop().time() + min(
            60, self.config.demo_run_timeout_seconds
        )
        while asyncio.get_running_loop().time() < deadline:
            current = self.repository.refresh(run.run_id)
            if current.processed_event_count >= current.generated_event_count:
                return
            await asyncio.sleep(self.config.demo_progress_refresh_interval_ms / 1000)
