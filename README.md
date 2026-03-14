<!-- README.md -->

<div align="center">

# 🏦 Banking CDC Data Pipeline

### A production-grade, event-driven data pipeline built on Change Data Capture (CDC) principles.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

</div>

---

## 🎯 One-Liner Pitch

> A fully containerized, **CDC-based** banking data pipeline that captures every INSERT, UPDATE, and **hard DELETE** from PostgreSQL — streaming changes through Kafka into a MinIO data lake, then loading and transforming them into a **Slowly Changing Dimension (SCD Type 2)** data mart in Snowflake via Airflow and dbt.

---

## 🏗️ High-Level Architecture


The data flows through **5 distinct stages**, each with a clear responsibility boundary:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  [1] INGEST         [2] CAPTURE        [3] BUFFER                       │
│                                                                         │
│  Faker Data    ──►  PostgreSQL    ──►  Debezium   ──►  Kafka Topics     │
│  Generator          (WAL=logical)      (CDC)            (3 topics)      │
│                                                                         │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  [3] BUFFER (cont.)    [4] LOAD              [5] TRANSFORM              │
│                                                                         │
│  Kafka Consumer  ──►  MinIO          ──►  Airflow DAG  ──►  dbt         │
│  (micro-batch)        (Parquet,            (every 10min)    Staging     │
│                        Hive partition)                   ──►  Snapshot  │
│                                                               (SCD2)    │
│                                                          ──►  Mart      │
│                                                               (dim/fact)│
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Walkthrough

| Stage | Component | Responsibility |
|---|---|---|
| **1. Ingest** | `Faker` + `psycopg2` | Simulates banking transactions. Generates `customers`, `accounts`, and `transactions` with referential integrity. Writes to PostgreSQL `raw` schema. |
| **2. Capture** | `Debezium` + `PostgreSQL WAL` | Reads the Write-Ahead Log as a replica. Publishes every row-level change event (INSERT/UPDATE/DELETE) to dedicated Kafka topics. Zero query load on the source DB. |
| **3. Buffer** | `kafka-python` consumer | Subscribes to 3 Debezium CDC topics. Micro-batches 50 records per topic, then serializes them as **Parquet** files with Hive-style date partitioning (`date=YYYY-MM-DD`) to MinIO. |
| **4. Load** | `Airflow DAG` (every 10 min) | Downloads Parquet files from MinIO, stages them to Snowflake internal stage (`@%table`), and executes `COPY INTO` to land data in the `RAW` schema. |
| **5. Transform** | `dbt` + `Airflow DAG` (daily) | Runs a 3-layer transformation: `Staging (View)` → `Snapshot (SCD Type 2)` → `Mart (Table / Incremental)`. Materializes analytics-ready `dim_customers`, `dim_accounts`, and `fact_transactions` in Snowflake `ANALYTICS` schema. |

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Generation** | Python, Faker, psycopg2 | Simulates realistic banking data with referential integrity |
| **Source DB** | PostgreSQL 15 | OLTP source; WAL logical replication enabled for CDC |
| **CDC Engine** | Debezium 2.2 | Captures row-level change events from PostgreSQL WAL |
| **Message Broker** | Apache Kafka (Confluent 7.4.0) | Durable, ordered event stream per table |
| **Data Lake** | MinIO (S3-compatible) | Parquet storage with Hive-style partitioning |
| **Orchestration** | Apache Airflow 2.9.3 | Schedules ingestion and dbt transformation DAGs |
| **Data Warehouse** | Snowflake | Analytical query layer; RAW + ANALYTICS schemas |
| **Transformation** | dbt Core + dbt-snowflake | Staging, SCD2 snapshots, and mart models |
| **Containerization** | Docker Compose | Full local environment reproducibility |
| **CI/CD** | GitHub Actions | Lint + compile on PR; dbt run + test on merge |
| **Package Manager** | uv (Astral) | Fast Python dependency resolution in Docker |

---

## 📁 Project Structure

```
adamxiang-banking-data-digestion-with-airflow-and-dbt/
│
├── .github/workflows/          # CI/CD pipeline definitions
│   ├── ci.yml                  #   ↳ Triggered on PR: ruff lint + dbt compile
│   └── cd.yml                  #   ↳ Triggered on merge to main: dbt run + dbt test
│
├── banking_dbt/                # dbt project root
│   ├── models/
│   │   ├── source.yml          #   ↳ Declares Snowflake RAW tables as dbt sources
│   │   ├── staging/            #   ↳ Layer 1: Views. Cleans & types raw Variant JSON
│   │   │   ├── stg_accounts.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_transactions.sql
│   │   └── mart/               #   ↳ Layer 3: Analytics-ready tables/incrementals
│   │       ├── dimensions/
│   │       │   ├── dim_accounts.sql    # SCD2 dimension sourced from snapshot
│   │       │   └── dim_customers.sql   # SCD2 dimension sourced from snapshot
│   │       └── facts/
│   │           └── fact_transactions.sql  # Incremental fact table
│   └── snapshots/              # Layer 2: SCD Type 2 history tracking
│       ├── accounts_snapshot.sql   # check_cols: [customer_id, account_type, balance]
│       └── customers_snapshot.sql  # check_cols: [first_name, last_name, email]
│
├── docker/dags/                # Airflow DAG definitions
│   ├── minio_to_snowflake.py   #   ↳ Every 10 min: MinIO Parquet → Snowflake RAW
│   └── dbt_snapshot.py         #   ↳ Daily: dbt snapshot → dbt run --select mart
│
├── scripts/
│   └── register_postgres_connector.py  # One-time Debezium connector registration
│
├── src/
│   ├── data_generator/         # Fake banking data simulator
│   │   ├── config.py           #   ↳ Tunable constants (batch size, amounts, etc.)
│   │   ├── data_generator.py   #   ↳ Pure data generation logic (no DB coupling)
│   │   ├── db_loader.py        #   ↳ Batch insert logic via psycopg2 execute_values
│   │   └── main.py             #   ↳ Orchestrator; wires generator → loader
│   ├── kafka_to_minio/         # Kafka CDC consumer & MinIO writer
│   │   ├── config.py
│   │   ├── kafka_client.py     #   ↳ KafkaConsumer initialization
│   │   ├── minio_client.py     #   ↳ Parquet serialization + S3 upload
│   │   └── main.py             #   ↳ Micro-batch consumption loop
│   └── SQL_DDL/                # PostgreSQL schema definitions
│       ├── customers.sql
│       ├── accounts.sql
│       └── transactions.sql
│
├── docker-compose.yml          # Full local stack definition (9 services)
├── Dockerfile                  # Airflow image + dbt-snowflake + uv
├── pyproject.toml              # Project metadata & data generator dependencies
├── requirements.txt            # Kafka consumer & Airflow DAG dependencies
└── .env.template               # Environment variable template (never commit .env)
```

---

## ✅ Prerequisites

Before you begin, ensure you have the following installed and configured:

- **Docker Desktop** (with at least 8 GB RAM allocated)
- **Python 3.12+**
- **uv** — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- A **Snowflake account** (the [30-day free trial](https://signup.snowflake.com/) is sufficient)
- A Snowflake database named `BANKING` with schemas `RAW` and `ANALYTICS` pre-created

---

## 🚀 Setup & Installation

### Step 1 — Clone the Repository & Configure Environment

```bash
git clone https://github.com/adamxiang/adamxiang-banking-data-digestion-with-airflow-and-dbt.git
cd adamxiang-banking-data-digestion-with-airflow-and-dbt

# Copy the template and fill in your credentials
cp .env.template .env
```

Open `.env` and populate all required values (Snowflake credentials, MinIO keys, Postgres password, Airflow DB credentials).

> ⚠️ **Important**: Update the `.dbt` volume mount path in `docker-compose.yml` to your local `~/.dbt` directory before proceeding.
> ```yaml
> # airflow-webserver & airflow-scheduler volumes:
> - /your/home/path/.dbt:/home/airflow/.dbt:ro  # ← change this
> ```

### Step 2 — Initialize Snowflake Tables

Log in to your Snowflake worksheet and execute the DDL found in `src/SQL_DDL/` to create the three source tables under the `RAW` schema.

```sql
-- Run in Snowflake UI (RAW schema context)
-- customers.sql → accounts.sql → transactions.sql (in order)
```

### Step 3 — Start All Docker Services

```bash
docker compose up -d
```

This command starts **9 services**: `zookeeper`, `kafka`, `kafka-connect` (Debezium), `postgres` (banking), `minio`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, and `airflow-postgres`.

Wait approximately 60–90 seconds for `airflow-init` to complete DB migration and user creation.

Verify all services are healthy:

```bash
docker compose ps
```

### Step 4 — Set Up dbt Profile

Create `~/.dbt/profiles.yml` with the following content, substituting your Snowflake credentials:

```yaml
banking_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: <your_user>
      password: <your_password>
      role: ACCOUNTADMIN
      database: BANKING
      warehouse: <your_warehouse>
      schema: ANALYTICS
```

### Step 5 — Register the Debezium PostgreSQL Connector

This is a **one-time setup** that instructs Debezium to start reading the PostgreSQL WAL and publishing CDC events to Kafka.

```bash
# Install script dependencies
uv pip install python-dotenv requests

# Register the connector (targets the Kafka Connect REST API on port 8083)
python scripts/register_postgres_connector.py
```

Verify the connector is running:

```bash
curl http://localhost:8083/connectors/postgres-connector/status | python -m json.tool
```

Expected: `"state": "RUNNING"`

### Step 6 — Start the Kafka-to-MinIO Consumer

In a **separate terminal**, start the micro-batch consumer. This process listens to the 3 Debezium Kafka topics and flushes records to MinIO as Parquet files every 50 messages.

```bash
cd src/kafka_to_minio
uv pip install kafka-python boto3 fastparquet pandas python-dotenv
python main.py
```

### Step 7 — Start the Data Generator

In another terminal, start the continuous data simulator:

```bash
cd src/data_generator
uv pip install faker psycopg2-binary python-dotenv
python main.py          # Runs continuously (every 2 seconds)
# or
python main.py --once   # Single iteration and exit
```

Each iteration generates **10 customers**, **20 accounts**, and **50 transactions**.

### Step 8 — Monitor & Verify

| Service | URL | Credentials |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | From your `.env` |
| **Kafka Connect REST** | http://localhost:8083 | — |

In the Airflow UI, you should see two active DAGs:
- `minio_to_snowflake_banking` — runs every 10 minutes
- `SCD2_snapshots` — runs daily

After ~10 minutes, verify data appears in your Snowflake `RAW` schema tables. After the daily DAG runs (or manually trigger it), verify `dim_customers`, `dim_accounts`, and `fact_transactions` are populated in the `ANALYTICS` schema.

---

## ⚙️ CI/CD Pipeline

This project implements a two-stage CI/CD strategy using GitHub Actions, designed to **minimize Snowflake compute costs** during validation.

```
Pull Request ──► CI Pipeline
                  ├── ruff check (linting & static analysis)
                  ├── pytest tests/ (unit tests)
                  └── dbt compile  ← validates SQL/Jinja WITHOUT running queries
                                     (no Snowflake Credits consumed)

Merge to main ──► CD Pipeline
                  ├── dbt deps
                  ├── dbt run    ← materializes tables/views in Snowflake PROD
                  └── dbt test   ← executes data quality assertions
```

Snowflake credentials are injected at runtime via **GitHub Secrets** (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`). They are never stored in the repository.

---

## 🧠 Key Design Decisions

### 1. Why CDC over Polling?

A simple `SELECT * WHERE updated_at > last_run` Airflow DAG would have been significantly easier to build. CDC was deliberately chosen for two reasons that matter at enterprise scale:

- **Hard Delete Capture**: Polling-based approaches are blind to `DELETE` operations. Debezium reads the PostgreSQL WAL as a replica, capturing deletes as first-class events.
- **Zero Source DB Intrusion**: Polling queries compete with OLTP workloads for CPU and I/O. Debezium attaches as a logical replica with negligible overhead on the source database.

### 2. Why `strategy='check'` for SCD Type 2 Snapshots?

The upstream data generator does not guarantee a database-trigger-managed `updated_at` field. The `timestamp` strategy would silently miss changes if the upstream timestamp is stale or missing. `strategy='check'` with explicitly declared `check_cols` (e.g., `balance`, `email`) is the **defensive choice** — it compares actual column values, making it immune to upstream timestamp unreliability.

**Known trade-off**: At millions of rows, column-by-column comparison consumes significant Snowflake compute credits. A production upgrade path would enforce a reliable `updated_at` upstream or introduce a hash-based comparison column.

### 3. Why `dbt compile` in CI instead of `dbt run`?

Running `dbt run` in CI would execute SQL against Snowflake on every pull request — consuming credits and introducing flaky test behaviour tied to cloud availability. `dbt compile` validates all Jinja macro resolution and SQL syntax entirely locally, providing fast, cost-free validation with zero cloud dependency.

---

## ⚠️ Known Limitations

- **Small File Problem**: With `BATCH_SIZE=50` and ~KB-sized JSON records, MinIO accumulates many small Parquet files. This is a classic Hadoop ecosystem anti-pattern that would require compaction (e.g., Spark or scheduled merge) in a production environment.
- **No Data Quality Layer**: The pipeline currently has no `dbt` schema tests or data contracts. Invalid data (e.g., negative balances) from upstream would propagate to Snowflake unchecked.
- **Single-node POC**: All services run on a single Docker host. No fault tolerance, replication, or auto-scaling is configured.

---

## 🔭 Future Work

- [ ] **Data Contracts**: Add `dbt` schema tests (`not_null`, `unique`, `accepted_values`, custom `check_balance_positive`) at the staging layer to enforce upstream data quality.
- [ ] **Apache Flink Integration**: Replace the Airflow ingestion DAG with a Flink job that consumes directly from Kafka, computes rolling aggregates (e.g., real-time account balance), and sinks to Snowflake — fully leveraging the low-latency CDC architecture.
- [ ] **File Compaction**: Implement a periodic Spark or dbt job to compact small Parquet files in MinIO into larger, query-optimized files.
- [ ] **Observability**: Integrate Grafana + Prometheus to monitor Kafka consumer lag, Airflow DAG SLA breaches, and Snowflake query performance.

---

## 🙏 Acknowledgement
- Data with Jay for providing this amazing project [tutorial](https://www.youtube.com/watch?v=uHiyZitmIS0) | [Linkedin](https://www.linkedin.com/in/jayachandrakadiveti/)

---

## 📄 License

This project is licensed under the MIT License.
