# Meridian Build Plan

## Component Checklist

- [x] Phase 1 — Repo structure + `.build_progress/`
- [ ] Component 1 — `pyproject.toml` + uv environment
- [ ] Component 2 — `infra/docker-compose.yml`
- [ ] Component 3 — Avro schemas + Pydantic models
- [ ] Component 4 — Synthetic event generator
- [ ] Component 5 — Flink Job 1: Revenue aggregator
- [ ] Component 6 — Flink Job 2: Fraud detector (CEP)
- [ ] Component 7 — Flink Job 3: Late event handler
- [ ] Component 8 — Flink Job 4: Inventory alerts (keyed state)
- [ ] Component 9 — quarantine-dq streaming integration
- [ ] Component 10 — Apache Iceberg sink
- [ ] Component 11 — dbt Core project
- [ ] Component 12 — DuckDB query layer
- [ ] Component 13 — Streamlit dashboard (5 pages)
- [ ] Component 14 — Prometheus + Grafana monitoring
- [ ] Component 15 — Makefile
- [ ] Test suite + benchmark
- [ ] README + CONCEPTS.md
- [ ] GitHub release v0.1.0

## Phase Sequence

1. **Orchestrator** — Repo init, folder structure, this plan
2. **Backend Architect** — Infra: pyproject, docker-compose, Avro schemas, Pydantic models
3. **Data Engineer** — Generator, Flink jobs (1-4), quarantine-dq, Iceberg sink, dbt, DuckDB
4. **DevOps/UI** — Streamlit dashboard, Prometheus, Grafana, Makefile
5. **QA** — Test suite, benchmark
6. **Evidence** — README, CONCEPTS.md, architecture diagram
7. **Reality Check** — End-to-end validation, GitHub release

## Decisions Log

| Date | Decision | Reason |
|------|----------|--------|
| 2026-05-20 | DuckDB adapter for dbt | Matches local development, no external warehouse needed |
| 2026-05-20 | MinIO as S3-compatible storage | Local S3 for Iceberg, zero cloud cost |
| 2026-05-20 | fastavro for Avro encoding | Faster than avro-python3, pure Python |
| 2026-05-20 | confluent-kafka for Schema Registry | Official Confluent client, best SR integration |
| 2026-05-20 | pyiceberg for Iceberg tables | Official Apache project, Python-native |
