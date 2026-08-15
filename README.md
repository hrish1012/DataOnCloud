# DataOnCloud

**A metadata-driven data ingestion and observability platform**, built to demonstrate production-grade data engineering practices: reusable batch/real-time ingestion frameworks, cloud-native storage, containerized microservices, scheduled orchestration, automated testing, and CI/CD — all running on Google Cloud Platform.

This isn't a tutorial project. Every component was built, broken, debugged, and fixed against real infrastructure — including a genuine Airflow memory-constraint crisis, a race condition in BigQuery's streaming API, and a CI pipeline that failed twice before passing. Those stories are documented below, because debugging real systems is the actual job.

---

## Architecture

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
 │  API sources │   │ File sources │   │  Event streams    │
 │  (REST APIs) │   │ (CSV / GCS)  │   │  (Pub/Sub)         │
 └──────┬───────┘   └──────┬───────┘   └────────┬──────────┘
        │                  │                    │
        └──────────────────┼────────────────────┘
                            ▼
              ┌──────────────────────────┐
              │   Ingestion Framework      │
              │   (Python, metadata-driven)│
              └─────────────┬─────────────┘
                            ▼
              ┌──────────────────────────┐
              │  Schema Validation Layer   │
              │  (valid / rejected split)  │
              └─────────────┬─────────────┘
                            ▼
              ┌──────────────────────────┐
              │  BigQuery (idempotent      │
              │  upsert via load jobs)     │
              └──────┬──────────────┬─────┘
                     ▼              ▼
          ┌────────────────┐  ┌──────────────────┐
          │ FastAPI Service │  │ Audit Log Table    │
          │ (REST endpoints)│  │ (lineage & history)│
          └────────────────┘  └────────┬───────────┘
                                        ▼
                              ┌──────────────────┐
                              │  Looker Studio     │
                              │  Dashboard          │
                              └──────────────────┘

        Orchestrated end-to-end by Apache Airflow (Docker, hourly schedule)
        Containerized with Docker · Tested with pytest · CI/CD via GitHub Actions
```

---

## What this project demonstrates

| Capability | Where it lives |
|---|---|
| Reusable batch + real-time ingestion framework | `src/ingestion/connectors/` — abstract base class, file/API/Pub-Sub connectors |
| Metadata-driven, config-based onboarding | `src/ingestion/config/sources.yaml` + factory pattern |
| Schema validation & data quality | `src/common/schema_validator.py` |
| Idempotent cloud data warehousing | `src/common/bq_loader.py` — MERGE-based upsert with change detection |
| Auditing & lineage | `ingestion_audit_log` BigQuery table, written on every run (success or failure) |
| Cloud-native microservice / REST API | `src/api/main.py` (FastAPI, parameterized queries, auto-generated docs) |
| Containerization | `Dockerfile.ingestion`, `Dockerfile.api` |
| Orchestration & scheduling | `dags/dataoncloud_ingestion_dag.py` (Airflow, `LocalExecutor`, hourly) |
| Observability | Structured JSON logging + Looker Studio dashboard on the audit table |
| Testing | `tests/` — 7 pytest tests covering validation logic and failure resilience |
| CI/CD | `.github/workflows/ci.yml` — tests run automatically on every push |

---

## Tech stack

**Language & core:** Python 3.12
**Cloud (GCP):** BigQuery, Pub/Sub, Cloud Storage, IAM Service Accounts
**API:** FastAPI + Uvicorn
**Orchestration:** Apache Airflow 2.9.1 (Docker Compose, LocalExecutor)
**Containerization:** Docker
**Testing:** pytest, pytest-mock
**CI/CD:** GitHub Actions
**Dashboard:** Looker Studio

---

## Running it locally

### 1. Ingestion framework (standalone)
```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
python run_test.py
```

### 2. Containerized
```bash
docker build -f Dockerfile.ingestion -t dataoncloud-ingestion .
docker run --rm -v "${PWD}/gcp-key.json:/app/gcp-key.json" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-key.json \
  -e GCP_PROJECT_ID=<dataoncloud-1012> \
  dataoncloud-ingestion
```

### 3. API service
```bash
docker build -f Dockerfile.api -t dataoncloud-api .
docker run --rm -p 8000:8000 -v "${PWD}/gcp-key.json:/app/gcp-key.json" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-key.json \
  -e GCP_PROJECT_ID=<dataoncloud> \
  dataoncloud-api
# → http://localhost:8000/docs
```

### 4. Orchestrated (Airflow)
```bash
docker-compose up -d
# → http://localhost:8080  (airflow / airflow)
```

### 5. Tests
```bash
pytest tests/ -v
```

> A `gcp-key.json` service account key and a `.env` file with `GCP_PROJECT_ID` and `GOOGLE_APPLICATION_CREDENTIALS` are required for anything touching real GCP resources. Neither is committed to this repo — see `.gitignore`.

---

## Engineering journey — real problems, real fixes

A few of the more genuine debugging stories from building this, kept here deliberately because they're a more honest signal of engineering ability than a clean feature list:

**BigQuery streaming-insert race condition.** The original upsert implementation created a staging table and immediately inserted into it via `insert_rows_json`, intermittently failing with `404 Table not found` — even right after the table was confirmed to exist via `get_table()`. Root cause: BigQuery's streaming insert API and its metadata API are backed by different, independently-consistent layers; a table can be "visible" to one before the other catches up. Fixed by switching to `load_table_from_json` (a load job), which doesn't share that consistency lag — not a longer timeout, an architecturally different write path.

**Change-detection upserts.** An early version of the `MERGE` statement updated every matched row unconditionally on every run, making "no changes" indistinguishable from "everything changed" in the audit log. Fixed by adding an `IS DISTINCT FROM`-based condition to `WHEN MATCHED`, so only genuinely changed rows trigger an update — verified by testing a true no-op run against a real edited row.

**Airflow under real memory constraints.** Running the default `CeleryExecutor` stack (webserver, scheduler, worker, triggerer, Redis, Postgres — 6 containers) on an 8GB laptop caused the Docker engine itself to become unresponsive under load, not just the containers. Diagnosed by ruling out the webserver first (its own health checks were passing throughout) before identifying the Docker daemon itself as the failure point. Resolved by switching to `LocalExecutor`, removing Redis and the separate worker entirely — a smaller footprint that's also the architecturally correct choice for a single-machine deployment, not just a workaround.

**CI pipeline, twice broken before green.** The first GitHub Actions run failed because a module-level `raise ValueError` in config loading executed on import, crashing test collection in an environment with no `.env` file. The second failure, after fixing that, was a mocking gap — `BigQueryLoader.load()` was mocked, but its `__init__` still attempted real GCP authentication. Both fixes are visible in the commit history rather than squashed away.

---

## Observability dashboard

A Looker Studio report built directly on top of the `ingestion_audit_log` BigQuery table — no separate monitoring stack, no extra containers, just a live view over the same audit data every pipeline run already writes.

**Layout:**
- **Three color-coded scorecards** at the top — Total Runs (blue), Successful Runs (green), Failed Runs (red) — giving an at-a-glance health check without opening a single log file.
- **A bar chart of rows loaded per source**, colored distinctly by `source_name`, showing ingestion volume across the file, API, and Pub/Sub connectors side by side.
- **A detailed run history table** — every field from the audit log (`source_name`, `status`, `started_at`, `rows_valid`, `rows_rejected`, `rows_loaded`, `error_message`) — with conditional row banding so failed runs are visibly flagged in red and successful runs in green, making it possible to spot a failure by scrolling, not querying.

Every color choice ties back to meaning (status or source category) rather than being decorative — the same "the log should tell you what happened, not just the number" principle from the framework's logging design, applied to a dashboard instead of a log line.

Since the dashboard reads directly from BigQuery, it updates automatically as Airflow's hourly DAG runs continue writing new audit records — no manual refresh or export step required.

Link to the dashboard : https://datastudio.google.com/reporting/f5b6f301-202b-4943-bace-e498a0c90ebb

---

## Possible extensions (not yet built)

- Alerting on ingestion failures (Cloud Monitoring or a webhook off `ingestion_audit_log`)
- A GitHub webhook as a genuine external event source, alongside the simulated Pub/Sub publisher
- Terraform for GCP resource provisioning
- Multi-task Airflow DAG with explicit inter-task dependencies

---

## Author

Built as a portfolio project to demonstrate the practical skills required for a Data Engineer II role: reusable ingestion frameworks, GCP-native pipelines, containerization, orchestration, observability, and CI/CD — developed and debugged end-to-end over three focused days.
