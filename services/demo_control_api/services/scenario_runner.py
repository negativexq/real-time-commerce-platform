"""Managed in-process execution through the existing generator interfaces."""

import asyncio
import random
from uuid import UUID

from services.demo_control_api.config import DemoConfig
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
        while generated < request.event_count:
            journey = builder.build()
            messages = injector.prepare(journey.events)
            remaining = request.event_count - generated
            messages = messages[:remaining]
            self.repository.add_manifest(
                run.run_id,
                [
                    (message.event_id, message.event_type)
                    for message in messages
                    if message.event_id
                ],
            )
            for message in messages:
                producer.publish_message(message)
                generated += 1
                self.repository.refresh(run.run_id, generated)
                if interval:
                    await asyncio.sleep(interval)
            producer.poll(0)
        producer.flush()

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
