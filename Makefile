.PHONY: up down producer jobs metrics dbt-run test lint clean demo help

COMPOSE = docker compose -f infra/docker-compose.yml

## ─── Infrastructure ─────────────────────────────────────────────────────────

up: ## Start all services
	$(COMPOSE) up -d
	@echo ""
	@echo "  Waiting for services to become healthy..."
	@sleep 20
	@echo ""
	@echo "  ✓ All services running:"
	@echo ""
	@echo "    Kafka UI:   http://localhost:8080"
	@echo "    Flink UI:   http://localhost:8082"
	@echo "    MinIO:      http://localhost:9001  (minioadmin / minioadmin)"
	@echo "    Grafana:    http://localhost:3000  (admin / admin)"
	@echo "    Dashboard:  http://localhost:8501"
	@echo "    Prometheus: http://localhost:9090"
	@echo ""

down: ## Stop all services and remove volumes
	$(COMPOSE) down -v

logs: ## Tail all service logs
	$(COMPOSE) logs -f

## ─── Data pipeline ───────────────────────────────────────────────────────────

producer: ## Start event generator (500 events/sec, 2% error rate)
	uv run python generator/producer.py --rate 500 --error-rate 0.02

producer-fast: ## Start event generator at 2000 events/sec
	uv run python generator/producer.py --rate 2000 --error-rate 0.02

jobs: ## Submit all 4 Flink jobs (runs in background)
	uv run python flink/jobs/revenue_aggregator.py &
	uv run python flink/jobs/fraud_detector.py &
	uv run python flink/jobs/late_event_handler.py &
	uv run python flink/jobs/inventory_alerts.py &
	@echo "Flink jobs started in background."

iceberg-init: ## Create Iceberg tables in MinIO
	uv run python iceberg/sink.py --init

metrics: ## Start Prometheus metrics server on :8888
	uv run python monitoring/metrics.py

dashboard: ## Start Streamlit dashboard locally (dev mode)
	uv run streamlit run dashboard/app.py --server.port 8501

## ─── dbt ─────────────────────────────────────────────────────────────────────

dbt-run: ## Run all dbt models and tests
	uv run dbt run --project-dir dbt_project --profiles-dir dbt_project
	uv run dbt test --project-dir dbt_project --profiles-dir dbt_project

dbt-docs: ## Generate and serve dbt documentation
	uv run dbt docs generate --project-dir dbt_project --profiles-dir dbt_project
	uv run dbt docs serve --project-dir dbt_project --profiles-dir dbt_project --port 8580

## ─── Quality ─────────────────────────────────────────────────────────────────

test: ## Run integration test suite
	uv run pytest tests/ -v --tb=short

test-e2e: ## Run end-to-end pipeline test only
	uv run pytest tests/test_end_to_end.py -v

benchmark: ## Run latency benchmark (1000 events, p50/p95 report)
	uv run python tests/benchmark.py

lint: ## Lint and format check
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-format all Python files
	uv run ruff format .

## ─── Quarantine ──────────────────────────────────────────────────────────────

replay: ## Replay all replayable quarantined events
	uv run python quarantine_int/replay_job.py --pipeline meridian

replay-dry: ## Dry-run replay (no writes)
	uv run python quarantine_int/replay_job.py --pipeline meridian --dry-run

## ─── Demo ────────────────────────────────────────────────────────────────────

demo: ## Full demo: start everything, then producer + jobs
	$(MAKE) up
	@sleep 30
	$(MAKE) iceberg-init
	@echo "Starting producer and Flink jobs..."
	$(MAKE) producer &
	@sleep 5
	$(MAKE) jobs
	@echo ""
	@echo "  Meridian is running! Open http://localhost:8501"
	@echo "  Press Ctrl+C to stop the producer."

## ─── Cleanup ─────────────────────────────────────────────────────────────────

clean: ## Remove generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name "*.pyo" -delete 2>/dev/null || true
	rm -rf dbt_project/target dbt_project/logs dbt_project/dbt_packages
	rm -rf .ruff_cache .pytest_cache .mypy_cache
	rm -f duckdb/meridian.duckdb duckdb/meridian.duckdb.wal

clean-quarantine: ## Clear the local quarantine store
	rm -rf quarantine_store/

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
