# Cloud Data Pipeline — Batch and Streaming Processing

An end-to-end hybrid data platform (Lambda Architecture) designed to integrate historical
transaction data (batch) and real-time transaction events (streaming) into a single data
warehouse.

The platform ensures the entire data lifecycle — raw ingestion, cleaning, validation, and
structured transformation through to analytic data marts — operates automatically, maintains
idempotency (protection against duplication), and is ready to support business analytics
needs.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Data Source Information](#data-source-information)
3. [Technology Stack](#technology-stack)
4. [Project Structure](#project-structure)
5. [Setup & Configuration](#setup--configuration)
6. [Running the Batch Pipeline](#running-the-batch-pipeline)
7. [Running the Streaming Pipeline](#running-the-streaming-pipeline)
8. [Data Quality Checks](#data-quality-checks)
9. [Idempotency Strategy](#idempotency-strategy)
10. [Partitioning & Clustering](#partitioning--clustering)
11. [Analytical Queries](#analytical-queries)
12. [Proof of Execution](#proof-of-execution)
13. [Assumptions](#assumptions)
14. [Cost Notes](#cost-notes)
15. [Known Limitations](#known-limitations)

---

## System Architecture

![System Architecture](docs/images/architecture.png)

The pipeline has two independent data flows that converge at the BigQuery warehouse:

- **Batch**: GCS (raw Parquet/CSV) → Airflow (`GCSToBigQueryOperator`) → BigQuery `raw` →
  dbt (`staging` → `intermediate` → `marts`)
- **Streaming**: Event generator → Pub/Sub → Apache Beam (validation + transform) →
  BigQuery `intermediate` (clean/quarantine)

---

## Data Source Information

| Category | Parameter | Details |
| :--- | :--- | :--- |
| **Batch Data** | Dataset | NYC Green Taxi Trip Records |
| | Processing Period | April – May 2026 |
| | Raw Records | ~89,159 rows |
| | Source Format | Parquet (trips) + CSV (zone lookup) |
| **Streaming Data** | Event Type | Simulated real-time trip transactions (JSON) |
| | Event Generator | Python publisher, sampling based on batch EDA |
| | Target Volume | ~89,159 events (June – July 2026, mirroring batch volume) |

---

## Technology Stack

| Category | Technology |
|---|---|
| Programming Language | Python 3.11 |
| Data Orchestration | Apache Airflow 2.10.5 |
| Data Transformation | dbt (BigQuery adapter) |
| Data Warehouse | Google BigQuery |
| Cloud Storage | Google Cloud Storage |
| Message Broker | Google Cloud Pub/Sub |
| Streaming Processing | Apache Beam (DirectRunner) |
| Containerization | Docker & Docker Compose |

---

## Project Structure

```text
cloud-data-pipeline
├── airflow/
│   └── dags/
│       └── batch_pipeline.py            # Main Airflow DAG (ingestion + dbt orchestration)
├── config/                              # Global settings (.env-backed) & local constants
├── dbt/                                 # dbt project (staging, intermediate, marts, analyses)
├── docs/
│   └── images/                          # Architecture diagram & execution proof screenshots
├── notebook/                            # Exploratory Data Analysis (EDA) on raw taxi dataset
├── scripts/
│   ├── batch/                           # GCS bucket setup & raw data upload helpers
│   └── streaming/                       # event_generator, publisher, Beam validation pipeline
├── utils/                               # Shared modules (logger, BigQuery schema, validation rules)
├── .env.example
├── .gitignore                           # Excludes .env, secrets/, data/, .venv/
├── docker-compose.yml                   # Local Airflow + Postgres metastore
├── requirements.txt
└── README.md
```

---

## Setup & Configuration

### Prerequisites

- Docker & Docker Compose
- Python 3.11 and a virtual environment (`.venv`)
- `gcloud` CLI, authenticated to your own GCP project
- A GCP service account key with BigQuery, GCS, and Pub/Sub access

### Steps

1. **Clone the repository and set up your own environment file**
   ```bash
   cp .env.example .env
   ```
   Fill in `.env` with **your own** values — your GCP project ID, a bucket name and
   BigQuery dataset names that include **your own** student ID, and your own Pub/Sub
   topic/subscription names. Never commit `.env` or any service-account JSON key — both
   are excluded via `.gitignore`.

2. **Install Python dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Authenticate to your own GCP account**
   ```bash
   gcloud auth login
   gcloud config set project <your-gcp-project-id>
   gcloud auth application-default login
   ```

4. **Provision your own GCP infrastructure** (bucket, BigQuery datasets/tables, Pub/Sub
   topic & subscription) using the values you set in your own `.env`:
   ```bash
   python3 -m scripts.batch.create_bucket                   # Create first bucket       
   python3 -m scripts.batch.download_upload_raw             # Download and Upload dataset Green Taxi to local and GCS bucket
   python3 -m scripts.streaming.setup_bigquery              # Create table for streaming processing
   gcloud pubsub topics create <your-topic-name>            # Create Topic for streaming processing 
   gcloud pubsub subscriptions create <your-subscription-name> --topic=<your-topic-name>    # Create Subriber for streaming pipeline
   ```

5. **Start Airflow locally**
   ```bash
   docker compose up -d
   ```
   Airflow UI: `http://localhost:8085`

---

## Running the Batch Pipeline

The batch pipeline is fully orchestrated by Airflow — no manual step is required.

- **DAG ID:** `agungnugraha_batch_trip_pipeline`
- **Schedule:** `@monthly`, backfilled for `2026-04-01` → `2026-05-31` (`catchup=True`,
  `max_active_runs=1` so each month completes fully before the next begins)
- **Flow:** `ingestion_layer` → `staging_layer` → `intermediate_layer` → `marts_layer`
  (each dbt layer runs `dbt run` then `dbt test`, parameterized by `reporting_year_month`
  derived from the DAG's `data_interval_start`)

Once unpaused in the Airflow UI, the DAG runs automatically according to its schedule. To
trigger a specific month manually, use the Airflow UI's "Trigger DAG w/ config" or:
```bash
airflow dags trigger agungnugraha_batch_trip_pipeline -e 2026-04-01
```

---

## Running the Streaming Pipeline

**1. EDA / profiling (run once, or whenever raw data changes)**
```bash
python3 -m scripts.streaming.batch_profiling
```
Produces `/data/profile/profile_output.json` — The statistical basis used by the event generator for sampling is derived from datasets from April and May, ensuring that the streaming data remains consistent with actual batch patterns rather than being purely random.

**2. Publisher** (in one terminal) — generates and sends events to Pub/Sub at a configurable rate:
```bash
python3 -m scripts.streaming.publisher --rate 2 --max-events 500
```
- `--rate`: events per second (default `1.0`)
- `--max-events`: optional cap; omit to run indefinitely
- Stop safely with `Ctrl+C` (handles `SIGINT`/`SIGTERM`, finishes the in-flight publish
  before exiting)

**3. Beam pipeline** (in a second terminal) — reads from Pub/Sub, validates, and writes to BigQuery:
```bash
python3 -m scripts.streaming.pipeline
```
This mode uses `DirectRunner` by default to process events directly in the local environment.

---

## Data Quality Validation

The batch and streaming pipelines use the same five core validation rules to ensure consistent data quality: record completeness, valid trip distance, valid fare amounts, valid passenger counts, and valid pickup/drop-off locations. Both pipelines also calculate `amount_mismatch` as an informational check.

Invalid records are not discarded. They are routed to dedicated quarantine tables (`int_quarantine_trips` for batch and `stream_trips_quarantine` for streaming) together with their failure reasons for auditing. The batch pipeline also performs duplicate detection, while streaming uses `event_id` for deduplication. Batch data quality is monitored through `dq_check_result`, and streaming freshness is monitored using dbt source freshness checks on `ingestion_time`.


---

## Idempotency Strategy

### Batch

* Pipeline execution is parameterized using `reporting_year_month`.
* Raw data uses `WRITE_TRUNCATE`, ensuring the same monthly data can be reloaded without creating duplicates.
* Staging and intermediate views hold no state of their own and are safe to re-run.
* `int_trips_enriched` uses a dbt incremental `merge` strategy with `trip_id` as the unique key, preventing duplicate records when the same month is reprocessed.
* `dq_check_result` uses an incremental `merge` strategy keyed by `reporting_period` and `rule_name`, so existing quality metrics are updated instead of duplicated.
* Mart tables are rebuilt on each run, replacing their existing contents.

### Streaming

* Every event contains a unique `event_id` for event identification.
* Invalid events are routed to the quarantine table with their validation failure reasons.
* Valid events are processed and written to the curated streaming layer.
* `event_id` can be used to identify duplicate events, but deduplication is not currently enforced at write time.


---

## Partitioning & Clustering

| Table                                            | Partition                      | Cluster                        | Reason                                                                                                                                                                                            |
| ------------------------------------------------ | ------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `raw_taxi_trip`                                  | `lpep_pickup_datetime` (MONTH) | `PULocationID`, `DOLocationID` | Monthly partitioning aligns with the batch processing grain and enables partition pruning for time-based queries. Zone columns are used frequently for downstream filtering, grouping, and joins. |
| `stream_trips_clean` / `stream_trips_quarantine` | `event_time` (DAY)             | None                           | Streaming data arrives continuously and is queried primarily by event time. Daily partitions reduce the amount of data scanned for time-scoped queries.                                           |


---

## Analytical Queries

1. **Batch-only** — any of the five marts (`daily_trips`, `hourly_demand`,
   `payment_behavior`, `route`, `zone_performance_summary`), built from
   `int_trips_enriched`.
2. **Batch vs. Streaming comparison** — `marts/batch_streaming_comparison.sql`, comparing
   `trip_count`, `avg_trip_distance`, `avg_fare_amount`, and `avg_total_amount` between
   `int_trips_enriched` (batch) and `stream_trips_clean` (streaming) side by side, to verify
   the generated streaming data is statistically consistent with real batch patterns.

---

## Proof of Execution

Screenshots are stored under `docs/images/`:

- `architecture.png` — system architecture diagram
- `dag_graph_success.png` — Airflow DAG graph, successful run
- `dbt_lineage_graph.png` — dbt model lineage
- `pubsub_topic_subscription.png` — Pub/Sub Console showing the topic and subscription
- `publisher_running.png` — publisher terminal log
- `beam_pipeline_running.png` — Beam pipeline terminal log
- `bigquery_stream_tables.png` — sample rows from `stream_trips_clean` / `stream_trips_quarantine`
- `bigquery_comparison_query.png` — result of `batch_streaming_comparison`
