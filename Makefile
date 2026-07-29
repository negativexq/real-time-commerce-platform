PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: lint format format-check type-check test check compose-config up down logs ps \
	kafka-topics kafka-describe kafka-smoke postgres-shell postgres-tables redis-cli \
	storage-smoke storage-status storage-logs generator-build generator-up \
	generator-down generator-logs generator-run generator-sample generator-status \
	generator-smoke generator-personas generator-normal generator-suspicious \
	generator-bot generator-takeover generator-anomalies generator-persona-smoke \
	generator-anomaly-smoke processor-build processor-up processor-down \
	processor-run processor-logs processor-status processor-sample processor-smoke \
	processor-duplicate-smoke processor-dlq-smoke processor-retry-smoke \
	processor-idempotency-status processor-clear-test-state clean clean-volumes \
	db-migrate db-migration-status db-schema-check db-tables db-counts \
	db-reset-test-data persistence-sample persistence-smoke \
	persistence-duplicate-smoke persistence-recovery-smoke \
	persistence-dependency-smoke persistence-refund-smoke \
	fraud-rules fraud-config-check fraud-db-status fraud-sample-normal \
	fraud-sample-suspicious fraud-sample-bot fraud-sample-takeover fraud-smoke \
	fraud-score-smoke fraud-alert-smoke fraud-idempotency-smoke \
	fraud-outbox-build fraud-outbox-up fraud-outbox-down fraud-outbox-logs \
	fraud-outbox-status fraud-outbox-smoke fraud-outbox-recovery-smoke \
	fraud-counts fraud-clear-test-data \
	observability-config-check observability-up observability-down \
	observability-restart observability-status observability-logs \
	prometheus-config-check prometheus-targets prometheus-rules-check \
	prometheus-query-smoke metrics-endpoints metrics-smoke grafana-health \
	grafana-dashboards-check observability-smoke observability-traffic \
	observability-reset-test-state

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

format-check:
	$(PYTHON) -m ruff format --check .

type-check:
	$(PYTHON) -m mypy .

test:
	$(PYTHON) -m pytest

check: lint format-check type-check test

compose-config:
	docker compose config

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f kafka kafka-init kafka-ui

ps:
	docker compose ps

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --list

kafka-describe:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --describe

kafka-smoke:
	docker compose run --rm --no-deps \
		-v "$(CURDIR)/scripts/kafka-smoke.sh:/opt/commerce/kafka-smoke.sh:ro" \
		--entrypoint /bin/bash kafka-init /opt/commerce/kafka-smoke.sh

postgres-shell:
	docker compose exec postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

postgres-tables:
	docker compose exec postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dt+"'

db-migrate:
	docker compose --profile processor run --rm postgres-migrate

db-migration-status:
	docker compose --profile processor run --rm --entrypoint python \
		postgres-migrate -m services.event_processor.persistence.migrations status

db-schema-check:
	docker compose --profile processor run --rm --entrypoint python \
		postgres-migrate -m services.event_processor.persistence.migrations check

db-tables: postgres-tables

db-counts:
	docker compose exec -T postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"SELECT relname AS table_name, n_live_tup AS estimated_rows FROM pg_stat_user_tables ORDER BY relname;"'

db-reset-test-data:
	@test -n "$(TEST_RUN_ID)" || (echo "TEST_RUN_ID is required" >&2; exit 2)
	docker compose exec -T postgres sh -c \
		'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" \
		-v run_id="$(TEST_RUN_ID)" -f /dev/stdin' < scripts/reset-persistence-test-data.sql

redis-cli:
	docker compose exec redis redis-cli

storage-smoke:
	./scripts/storage-smoke.sh

storage-status:
	docker compose ps postgres redis

storage-logs:
	docker compose logs -f postgres redis

generator-build:
	docker compose --profile generator build event-generator

generator-up:
	docker compose --profile generator up -d --build event-generator

generator-down:
	docker compose --profile generator rm -sf event-generator

generator-logs:
	docker compose --profile generator logs -f event-generator

generator-run:
	docker compose --profile generator run --rm event-generator

generator-sample:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --seed 42

generator-status:
	docker compose --profile generator ps event-generator

generator-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-smoke.py

generator-personas:
	@printf '%s\n' \
		'Supported personas: normal, indecisive, discount_hunter, suspicious, bot, account_takeover' \
		'Configured weights: $(or $(GENERATOR_PERSONA_WEIGHTS),normal=0.50,indecisive=0.15,discount_hunter=0.15,suspicious=0.10,bot=0.05,account_takeover=0.05)'

generator-normal:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --persona normal --seed 42

generator-suspicious:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --persona suspicious --seed 43

generator-bot:
	docker compose --profile generator run --rm event-generator \
		--journeys 2 --persona bot --seed 44

generator-takeover:
	docker compose --profile generator run --rm event-generator \
		--journeys 2 --persona account_takeover --seed 45

generator-anomalies:
	docker compose --profile generator run --rm \
		-e GENERATOR_DUPLICATE_EVENT_PROBABILITY=1 \
		-e GENERATOR_MALFORMED_JSON_PROBABILITY=1 \
		-e GENERATOR_MISSING_FIELD_PROBABILITY=1 \
		-e GENERATOR_UNKNOWN_EVENT_TYPE_PROBABILITY=1 \
		-e GENERATOR_LATE_EVENT_PROBABILITY=1 \
		-e GENERATOR_OUT_OF_ORDER_PROBABILITY=1 \
		-e GENERATOR_PAYLOAD_MISMATCH_PROBABILITY=1 \
		-e GENERATOR_MAX_ANOMALIES_PER_JOURNEY=7 \
		event-generator --journeys 1 --persona normal --anomalies --seed 5150

generator-persona-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-persona-smoke.py

generator-anomaly-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-anomaly-smoke.py

processor-build:
	docker compose --profile processor build event-processor

processor-up:
	docker compose --profile processor up -d --build event-processor

processor-down:
	docker compose --profile processor rm -sf event-processor

processor-run:
	docker compose --profile processor run --rm event-processor

processor-logs:
	docker compose --profile processor logs -f event-processor

processor-status:
	docker compose --profile processor ps event-processor

processor-sample:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py normal

processor-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py normal

processor-duplicate-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py duplicate

processor-dlq-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py dlq

processor-retry-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py retry

persistence-sample:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py sample

persistence-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py normal

persistence-duplicate-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py duplicate

persistence-recovery-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py recovery

persistence-dependency-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py dependency

persistence-refund-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py refund

fraud-rules:
	$(PYTHON) scripts/fraud-admin.py rules

fraud-config-check:
	$(PYTHON) scripts/fraud-admin.py config

fraud-db-status fraud-counts:
	docker compose exec -T postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"SELECT (SELECT COUNT(*) FROM fraud_evaluations) AS evaluations, \
		(SELECT COUNT(*) FROM fraud_alerts WHERE status = '"'"'OPEN'"'"') AS open_alerts, \
		(SELECT COUNT(*) FROM fraud_outbox WHERE status = '"'"'PENDING'"'"') AS pending, \
		(SELECT COUNT(*) FROM fraud_outbox WHERE status = '"'"'PUBLISHED'"'"') AS published;"'

fraud-sample-normal:
	$(MAKE) generator-normal

fraud-sample-suspicious:
	$(MAKE) generator-suspicious

fraud-sample-bot:
	$(MAKE) generator-bot

fraud-sample-takeover:
	$(MAKE) generator-takeover

fraud-smoke fraud-score-smoke fraud-alert-smoke fraud-idempotency-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_engine.py

fraud-outbox-build:
	docker compose --profile fraud build fraud-outbox-publisher

fraud-outbox-up:
	docker compose --profile fraud up -d --build fraud-outbox-publisher

fraud-outbox-down:
	docker compose --profile fraud rm -sf fraud-outbox-publisher

fraud-outbox-logs:
	docker compose --profile fraud logs -f fraud-outbox-publisher

fraud-outbox-status:
	docker compose --profile fraud ps fraud-outbox-publisher

fraud-outbox-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_engine.py \
		-k alert_event_is_deterministic

fraud-outbox-recovery-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_outbox.py

fraud-clear-test-data:
	docker compose exec -T postgres sh -c \
		'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') \
		DELETE FROM fraud_outbox WHERE aggregate_id IN \
		(SELECT alert_id FROM fraud_alerts WHERE source_event_id IN \
		(SELECT event_id FROM test_events)); \
		WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') DELETE FROM fraud_alerts \
		WHERE source_event_id IN (SELECT event_id FROM test_events); \
		WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') DELETE FROM fraud_evaluations \
		WHERE source_event_id IN (SELECT event_id FROM test_events);"'

prometheus-config-check:
	docker compose --profile observability run --rm --no-deps \
		--entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml

prometheus-rules-check:
	docker compose --profile observability run --rm --no-deps \
		--entrypoint promtool prometheus check rules \
		/etc/prometheus/rules/recording.yml \
		/etc/prometheus/rules/demo-alerts.yml

observability-config-check: compose-config prometheus-config-check prometheus-rules-check
	$(PYTHON) -m json.tool infra/observability/grafana/dashboards/platform-overview.json >/dev/null
	@for dashboard in infra/observability/grafana/dashboards/*.json; do \
		$(PYTHON) -m json.tool "$$dashboard" >/dev/null || exit 1; \
	done
	@grep -q 'uid: commerce-prometheus' \
		infra/observability/grafana/provisioning/datasources/prometheus.yml
	@grep -q '/var/lib/grafana/dashboards' \
		infra/observability/grafana/provisioning/dashboards/dashboards.yml

observability-up:
	docker compose --profile observability up -d \
		kafka-exporter postgres-exporter redis-exporter prometheus grafana

observability-down:
	docker compose --profile observability stop \
		grafana prometheus kafka-exporter postgres-exporter redis-exporter

observability-restart: observability-down observability-up

observability-status:
	docker compose --profile observability ps \
		kafka-exporter postgres-exporter redis-exporter prometheus grafana

observability-logs:
	docker compose --profile observability logs -f \
		kafka-exporter postgres-exporter redis-exporter prometheus grafana

prometheus-targets:
	@curl -fsS "http://127.0.0.1:$${PROMETHEUS_PORT:-9090}/api/v1/targets" | \
		$(PYTHON) -c 'import json,sys; d=json.load(sys.stdin); t=d["data"]["activeTargets"]; print(*[(x["labels"].get("job"),x["health"]) for x in t],sep="\n"); raise SystemExit(any(x["health"]!="up" for x in t if x["labels"].get("job") in {"prometheus","kafka-exporter","postgres-exporter","redis-exporter"}))'

prometheus-query-smoke:
	@curl -fsS --get "http://127.0.0.1:$${PROMETHEUS_PORT:-9090}/api/v1/query" \
		--data-urlencode 'query=sum(up)' | $(PYTHON) -m json.tool

metrics-endpoints:
	docker compose --profile processor exec -T event-processor \
		python -c 'import urllib.request; body=urllib.request.urlopen("http://localhost:9101/metrics").read(); assert b"commerce_processor_events_received_total" in body'
	docker compose --profile fraud exec -T fraud-outbox-publisher \
		python -c 'import urllib.request; body=urllib.request.urlopen("http://localhost:9103/metrics").read(); assert b"commerce_outbox_publications_total" in body'

metrics-smoke: observability-traffic
	@sleep 12
	@curl -fsS --get "http://127.0.0.1:$${PROMETHEUS_PORT:-9090}/api/v1/query" \
		--data-urlencode 'query=sum(commerce_processor_events_received_total)' | \
		$(PYTHON) -c 'import json,sys; d=json.load(sys.stdin); assert d["status"]=="success"'
	@! rg -n 'customer_id|event_id|order_id|payment_id|correlation_id|device_id|email_hash|ip_address' \
		shared/observability infra/observability/grafana

grafana-health:
	curl -fsS "http://127.0.0.1:$${GRAFANA_PORT:-3002}/api/health"

grafana-dashboards-check:
	@curl -fsS -u "$${GRAFANA_ADMIN_USER:-admin}:$${GRAFANA_ADMIN_PASSWORD:-commerce_local_dev}" \
		"http://127.0.0.1:$${GRAFANA_PORT:-3002}/api/search?type=dash-db" | \
		$(PYTHON) -c 'import json,sys; d=json.load(sys.stdin); u={x["uid"] for x in d}; e={"commerce-platform-overview","commerce-kafka-streaming","commerce-processor","commerce-persistence","commerce-fraud","commerce-outbox","commerce-infrastructure"}; print(*sorted(u),sep="\n"); assert e <= u'

observability-traffic:
	$(MAKE) generator-normal
	$(MAKE) generator-takeover
	$(MAKE) generator-anomalies

observability-smoke: observability-config-check
	docker compose --profile processor --profile fraud --profile observability \
		up -d --build event-processor fraud-outbox-publisher prometheus grafana
	$(MAKE) observability-traffic
	@sleep 15
	$(MAKE) prometheus-targets
	$(MAKE) prometheus-query-smoke
	$(MAKE) grafana-health
	$(MAKE) grafana-dashboards-check

observability-reset-test-state:
	@test -n "$(OBSERVABILITY_TEST_RUN_ID)" || \
		(echo "OBSERVABILITY_TEST_RUN_ID is required" >&2; exit 2)
	$(MAKE) db-reset-test-data TEST_RUN_ID="$(OBSERVABILITY_TEST_RUN_ID)"
	@docker compose exec -T redis sh -c \
		'redis-cli --scan --pattern "commerce:processor:observability:$(OBSERVABILITY_TEST_RUN_ID):*" | xargs -r redis-cli DEL >/dev/null'
	@printf '%s\n' 'Removed only explicitly scoped observability test state.'

processor-idempotency-status:
	docker compose exec -T redis redis-cli --scan \
		--pattern 'commerce:processor:*' | head -100

processor-clear-test-state:
	@docker compose exec -T redis sh -c \
		'redis-cli --scan --pattern "commerce:processor:test:*" | \
		xargs -r redis-cli DEL >/dev/null'
	@printf '%s\n' 'Deleted only commerce:processor:test:* Redis keys.'

clean:
	docker compose down

clean-volumes:
	@printf '%s\n' \
		'WARNING: deleting all persisted Kafka, PostgreSQL, and Redis data.'
	docker compose down --volumes

# Sprint 10 interactive demo control center. All scenario targets use the API.
DEMO_PROFILES = --profile processor --profile fraud --profile observability --profile demo
DEMO_RUN_ID ?=

demo-build:
	docker compose $(DEMO_PROFILES) build demo-control-api demo-control-web

demo-up:
	docker compose $(DEMO_PROFILES) up -d --build

demo-down:
	docker compose $(DEMO_PROFILES) stop demo-control-web demo-control-api

demo-restart:
	docker compose $(DEMO_PROFILES) restart demo-control-api demo-control-web

demo-status:
	docker compose $(DEMO_PROFILES) ps

demo-logs:
	docker compose $(DEMO_PROFILES) logs -f demo-control-api demo-control-web

demo-api-health:
	curl -fsS http://127.0.0.1:$${DEMO_API_HOST_PORT:-8082}/api/v1/health

demo-web-health:
	curl -fsS http://127.0.0.1:$${DEMO_WEB_HOST_PORT:-3003}/

demo-config-check:
	docker compose $(DEMO_PROFILES) config --quiet

demo-db-status:
	$(MAKE) db-migration-status

demo-scenarios:
	curl -fsS http://127.0.0.1:$${DEMO_API_HOST_PORT:-8082}/api/v1/scenarios

demo-run-normal:
	$(PYTHON) scripts/demo-run.py normal_customer
demo-run-suspicious:
	$(PYTHON) scripts/demo-run.py suspicious_payment
demo-run-takeover:
	$(PYTHON) scripts/demo-run.py account_takeover
demo-run-bot:
	$(PYTHON) scripts/demo-run.py bot_checkout
demo-run-refund:
	$(PYTHON) scripts/demo-run.py refund_abuse
demo-run-duplicate:
	$(PYTHON) scripts/demo-run.py duplicate_delivery
demo-run-malformed:
	$(PYTHON) scripts/demo-run.py malformed_event
demo-run-mixed:
	$(PYTHON) scripts/demo-run.py mixed_traffic

demo-run-status:
	@test -n "$(DEMO_RUN_ID)" || (echo "DEMO_RUN_ID is required" >&2; exit 2)
	curl -fsS "http://127.0.0.1:$${DEMO_API_HOST_PORT:-8082}/api/v1/runs/$(DEMO_RUN_ID)/summary"

demo-stop:
	@test -n "$(DEMO_RUN_ID)" || (echo "DEMO_RUN_ID is required" >&2; exit 2)
	curl -fsS -X POST "http://127.0.0.1:$${DEMO_API_HOST_PORT:-8082}/api/v1/runs/$(DEMO_RUN_ID)/stop"

demo-ui-smoke:
	$(PYTHON) scripts/demo-ui-smoke.py

demo-clean-test-run:
	@test -n "$(DEMO_RUN_ID)" || (echo "DEMO_RUN_ID is required" >&2; exit 2)
	curl -fsS -X DELETE "http://127.0.0.1:$${DEMO_API_HOST_PORT:-8082}/api/v1/runs/$(DEMO_RUN_ID)/test-data"

demo-smoke: demo-api-health demo-web-health demo-scenarios demo-run-normal demo-run-takeover demo-run-duplicate demo-run-malformed demo-ui-smoke

demo-full: demo-up
	$(MAKE) demo-api-health
	$(MAKE) demo-web-health
	$(MAKE) demo-run-mixed
	@printf '%s\n' 'Full demo stack remains running.'

demo-verification-processor-up:
	docker compose --profile processor stop event-processor
	docker compose --profile processor --profile fraud --profile demo \
		--profile demo-verification up -d --build demo-verification-processor

demo-verification-processor-ready:
	@attempt=0; while [ $$attempt -lt 30 ]; do \
		output="$$(docker compose exec -T kafka \
			/opt/kafka/bin/kafka-consumer-groups.sh \
			--bootstrap-server kafka:9092 \
			--describe --group commerce-demo-verification-v1 \
			--members --verbose 2>/dev/null || true)"; \
		if printf '%s\n' "$$output" | awk \
			'$$1 == "commerce-demo-verification-v1" && $$5 + 0 == 3 {found=1} END {exit !found}'; then \
			printf '%s\n' "$$output"; exit 0; \
		fi; \
		attempt=$$((attempt + 1)); sleep 1; \
	done; \
	echo "verification processor did not receive all three partitions" >&2; exit 1

demo-verification-takeover:
	$(PYTHON) scripts/demo-verification-takeover.py

demo-verification-processor-down:
	docker compose --profile processor --profile demo-verification \
		rm -sf demo-verification-processor
	docker compose --profile processor start event-processor
