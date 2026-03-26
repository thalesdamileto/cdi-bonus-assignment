# CDI Bonus Assignment — Data Product (PySpark + Delta Lake)

This repository implements a **medallion-style data pipeline** for the **CDI Bonus** challenge: ingest simulated wallet CDC-style transactions, derive per-account balances split by CDI eligibility (time-based rule), compute daily CDI-style accrual, and persist **audit**, **payout**, and **current balance** outputs in **Delta Lake** on local disk.

The runnable local entrypoint is `app/main.py` (Bronze → Silver → Gold). Notebook-style sources under `app/*_pipeline/*_from_*.py` preserve the original **Databricks** structure (`# Databricks notebook source`, `dbutils`, Unity Catalog `saveAsTable`); the **`local_*`** and **`gold_silver`** modules implement the same ideas using **path-based Delta** reads/writes.

---

## Assignment context (summary)

**Functional goals**

- **Wallet history:** Build an intermediate history from CDC-like updates so balances can be reasoned about over time.
- **Interest (CDI bonus):** Accrue on cash that qualifies; assignment specifies rules such as **balance above $100**, **funds stationary for at least 24 hours**, **rate can change daily**, **calendar day (00:00–23:59)**, and **daily payout** to qualifying users.

**Non-functional goals**

- Treat as **mission-critical**: observability, data quality signals, and **clear documentation** of business logic.

**Technical requirement**

- **Spark** is used for the main calculation and file I/O (Parquet/Delta).

---

## End-to-end pipeline flow

```text
data/raw/fake_transactions/part-*.parquet  (CDC-like events)
        │
        ▼  Bronze — append Delta, merge schema
data/bronze/bronze_transactions
        │
        ▼  Silver — aggregate by account; split “CDI applicable” vs not (time cutoff)
data/silver/transactions
        │
        ▼  Gold — apply rate, decimal money types, three Delta outputs
data/gold/transactions/
    ├── daily_cdi_stagin_audit/      (full audit row per run)
    ├── daily_cdi_bonus/             (account_id, cdi_bonus, cdi_bonus_date)
    └── account_balance_last_day_cdi/  (merge: current_balance, last_day_bonus)
```

### Bronze (`app/bronze_pipeline/`)

- **Databricks reference:** `bronze_from_parquet.py` reads **Parquet** from a source path and was intended to land in **Unity Catalog**; comments note **local** use: `write.format("delta").mode("append").save(bronze_path)` with `mergeSchema`.
- **Local implementation:** `local_bronze.py` reads Parquet from `data/raw/fake_transactions/part-*.parquet`, applies data-quality checks, writes invalid rows to `data/bronze/quarantine`, and **appends only valid rows** to `data/bronze/bronze_transactions` as Delta with **schema evolution** enabled.

  Data-quality checks in Bronze:
  - `user_id` must be non-null
  - `account_id` must be non-null
  - `transaction_type` must be one of:
    - `WITHDRAWAL`
    - `TRANSFER_OUT`
    - `TRANSFER_IN`
    - `DEPOSIT`
    - `WALLET_CREATED`

**Role:** Immutable-style landing of raw events in a queryable, versioned store (Delta).

### Silver (`app/silver_pipeline/`)

- **Databricks reference:** `silver_from_bronze.py` describes **streaming** from Bronze (`readStream` / `writeStream` with checkpoint) and writing to a catalog table; comments mark **TODOs** for local Delta paths instead of `toTable(...)`. It also discusses a **`time_limit`** aligned with **“24 hours before max event_time rounding to midnight”** (prototype uses a fixed ISO timestamp). The **pivot** on `cdi_applicable` with explicit `[True, False]` is called out for **performance**.
- **Local implementation:** `local_silver.py` runs a **batch** read from Bronze Delta. For each row it sets **`cdi_applicable`** when `event_time` is **before** the configured `time_limit` (proxy for the **“stationary for 24h”** window relative to a processing cutoff). It **groups by `account_id`**, **pivots** applicable vs not, and produces:

  - `total_amount`
  - `amount_cdi_applicable`
  - `amount_cdi_not_applicable`

  If the Silver table already exists, it **merges** upserted aggregates by `account_id` using **Delta `MERGE`**.

**Role:** Intermediate **wallet / balance state** table used as input to CDI calculation (history-oriented layer in the medallion sense).

### Gold (`app/gold_pipeline/`)

- **Databricks reference:** `gold_from_silver.py` documents the **business logic** and **target tables**:

  - **`balance_to_apply_cdi`:** if `amount_cdi_not_applicable < 0`, use `amount_cdi_applicable + amount_cdi_not_applicable`; else use `amount_cdi_applicable` (adjusts the base when the “not applicable” bucket is negative).
  - **Rate:** comment states **~0.055% daily** (`0.00055`) and notes it **should come from a table or API** in production.
  - **Money:** **`decimal(18, 2)`** for bonus and final balance to **reduce rounding drift** when handling money.
  - **Outputs (conceptual):**
    - **Table 1:** `account_id`, `cdi_bonus`, `cdi_bonus_date` — noted for **federal tax** style reporting.
    - **Table 2:** `account_id`, `current_balance`, `last_day_bonus`.
    - **Audit:** append full **`cdi_apply_df`** for lineage/audit.

- **Local implementation:** `gold_silver.py` mirrors this: reads Silver Delta, builds the same derived columns, writes:

  | Path under `data/gold/transactions/` | Purpose |
  |----------------------------------------|---------|
  | `daily_cdi_stagin_audit/` | Append-only audit of the enriched calculation rowset |
  | `daily_cdi_bonus/` | Append bonus payouts with `current_timestamp()` as `cdi_bonus_date` |
  | `account_balance_last_day_cdi/` | **Merge** latest `current_balance` and `last_day_bonus` per account |

---

## Repository layout (data + code)

| Path | Description |
|------|-------------|
| `data/raw/fake_transactions/` | Input Parquet (wallet-style CDC simulation). |
| `data/bronze/bronze_transactions/` | Delta: Bronze append of valid raw events. |
| `data/bronze/quarantine/` | Delta: Bronze quarantine for invalid raw events (null IDs or disallowed `transaction_type`). |
| `data/silver/transactions/` | Delta: per-account aggregates + CDI applicability split. |
| `data/gold/transactions/*` | Delta: audit, daily bonus, merged account balances. |
| `app/main.py` | Orchestrates Bronze → Silver → Gold locally. |
| `app/helpers/spark_helpers.py` | Spark + Delta via `delta-spark` pip; `JAVA_HOME` helper. |
| `app/helpers/general_helpers.py` | Simple `log()` for visibility. |
| `tests/` | Pytest suite: repository path resolution, Silver/Gold transforms, `log()`. |
| `pyspark_notebook.py` | Optional **read-only** inspection of raw Parquet and Delta tables (`show` / counts). |
| `app/bronze_pipeline/bronze_from_parquet.py` | Databricks notebook source (reference). |
| `app/silver_pipeline/silver_from_bronze.py` | Databricks notebook source (reference). |
| `app/gold_pipeline/gold_from_silver.py` | Databricks notebook source (reference). |

---

## Prerequisites

- **Python** 3.11 or 3.12 (see `pyproject.toml`).
- **Java 17** (PySpark requires a JDK). Set `JAVA_HOME`, or install OpenJDK 17, or use a JDK under `~/.local/share/java/jdk-17*` as supported by `ensure_java_home()` in `app/helpers/spark_helpers.py`.
- **Poetry** (recommended) or another way to install dependencies from `pyproject.toml` / `poetry.lock`.
- **Network** on first Spark + Delta use: `configure_spark_with_delta_pip` may resolve JARs via Ivy/Maven.

### Docker Desktop on Windows (step-by-step)

Use this path if you want to run the project on Windows without installing Python, Java, or Poetry locally.

1. Install **Docker Desktop** and make sure it is running with **Linux containers**.
2. Open **PowerShell** in the repository root folder (where `Dockerfile` and `docker-compose.yml` are).
3. Build the image and run the pipeline:

```powershell
docker compose up --build
```

4. Wait for logs indicating Bronze, Silver, and Gold finished successfully.
5. Check output files on Windows in:
   - `data/bronze/bronze_transactions`
   - `data/bronze/quarantine`
   - `data/silver/transactions`
   - `data/gold/transactions`

Why data persists on Windows host:

- `docker-compose.yml` maps `./data` (host) to `/app/data` (container), so generated Delta files remain in your project folder after the container exits.

Useful commands (PowerShell):

```powershell
# Run in detached mode
docker compose up --build -d

# See service logs
docker compose logs -f pipeline

# Stop and remove container/network
docker compose down
```

If Docker reports daemon/context issues, restart Docker Desktop and rerun the command in the same project directory.

---

## Install dependencies

From the repository root:

```bash
poetry install
```

This installs **PySpark 3.5.3** and **delta-spark 3.2.0** (aligned versions matter for the Delta connector).

---

## Run the pipeline

Always run from the **repository root** so imports resolve.

**Recommended (module execution):**

```bash
python -m app.main
```

**Direct script (requires `PYTHONPATH`):**

```bash
export PYTHONPATH="$(pwd)"
poetry run python app/main.py
```

The pipeline runs **Bronze → Silver → Gold** in one process and stops the Spark session in a `finally` block. Logs are printed via `log(...)`.

**With Docker (Windows/Linux/macOS):**

```bash
docker compose up --build
```

This uses `Dockerfile` + `docker-compose.yml` and runs `python -m app.main` inside the container.

**Inspect existing tables (optional):**

```bash
poetry run python pyspark_notebook.py
```

(Open-source Spark has no Databricks `display()`; the script uses `show()` and counts, consistent with the comment in `pyspark_notebook.py`.)

---

## Tests

**Automated (pytest):** from the repository root, after `poetry install`:

```bash
poetry run pytest
```

What is covered:

- **`_pipeline_stages_with_repo_paths`** in `app/main.py` — Bronze/Silver/Gold paths are absolute and rooted at the repo; non-path settings match `PIPELINE_CONFIG`.
- **`_build_silver_delta`** — CDI applicability split by `time_limit` and per-account totals.
- **`_build_gold_frames`** — negative “not applicable” adjustment, **$100** minimum for CDI bonus, and `decimal(18, 2)` bonus/balance columns.
- **`log()`** in `app/helpers/general_helpers.py` — default and error levels.

Delta **merge/append** I/O is not unit-tested here (heavier, path-dependent); validate end-to-end with the steps below.

**Manual checks:**

- Run `python -m app.main` and confirm log lines for each step without exceptions.
- Run `pyspark_notebook.py` and confirm row counts and samples for Bronze/Silver/Gold paths.

---

## Recommendations: loading results into a production transactional database

These align with **money**, **auditability**, and **mission-critical** operations:

1. **Treat Gold outputs as facts, not the ledger of record until validated**  
   Use **`daily_cdi_bonus`** as an **append-only** feed of proposed payouts. Load into the wallet DB only after **idempotent** application (e.g. natural key: `account_id` + **business payout date** + batch id).

2. **Idempotency and concurrency**  
   Orchestrate **at-least-once** file delivery with **deduplication** in the DB (unique constraint + `ON CONFLICT` / equivalent). Never double-credit a user on pipeline retries.

3. **Ingest pattern**  
   - **Batch:** periodic job reads new partitions from Delta (or export Parquet/CSV to object storage) and uses **bulk load** + a **staging table**, then **single transactional MERGE** into live balances.  
   - **Streaming:** if latency matters, use Spark Structured Streaming or a CDC tool from Delta to a **change queue** consumed by a wallet service with **exactly-once** semantics at the application layer.

4. **Schema contract**  
   Publish a **versioned contract** for `daily_cdi_bonus` and `account_balance_last_day_cdi` (types, nullability, timezone for `cdi_bonus_date`). Enforce **`decimal(18,2)`** (or DB `NUMERIC`) end-to-end.

5. **Observability**  
   Metrics: rows read/written per layer, **null rate** on money columns, **merge** counts, job duration. Alerts on **anomalies** (sudden drop in volume, negative bonuses where disallowed). Use **`daily_cdi_stagin_audit`** for **forensic replay** when users dispute payouts.

6. **Source data quality**  
   Before Gold, run **expectations** (Great Expectations, Deequ, or Delta constraints) on Bronze/Silver: duplicate CDC sequence keys, impossible timestamps, balance monotonicity checks where applicable.

---

## Design choices (how this meets functional and non-functional requirements)

| Topic | Choice | Rationale |
|-------|--------|-----------|
| **Spark** | All heavy transforms and I/O in Spark | Matches the assignment and scales to large CDC volumes. |
| **Delta Lake** | Bronze/Silver/Gold on disk | **ACID**, **time travel**, **schema evolution**, **merge** for upserts—appropriate for money-adjacent pipelines and audit. |
| **Medallion** | Bronze → Silver → Gold | Separates **raw landing**, **interpreted business state**, and **publishable** artifacts—eases debugging and SLAs per layer. |
| **Money types** | `decimal(18, 2)` in Gold | As in the Databricks notebook comments: limits floating-point error in bonus and balances. |
| **Visibility** | `log()` at step boundaries; audit Delta table | Supports operational awareness; full calculation rowset retained in **`daily_cdi_stagin_audit`**. |
| **Databricks vs local** | Notebooks kept as reference; `local_*` + `gold_silver` for execution | Clear **porting path**: swap paths for `saveAsTable` / Unity Catalog when deploying to Databricks. |

---

## Trade-offs and gaps (time / scope)

- **Streaming:** The Databricks Silver notebook targets **Delta streaming** with checkpoints; the local path uses **batch** reads/writes for simplicity and reproducibility on a laptop.
- **Fixed `time_limit`:** Silver uses a **constant** ISO timestamp instead of `datetime` “yesterday at midnight” as suggested in comments—easier for deterministic demos; production should compute the cutoff from **watermark** / **business date**.
- **CDI rate:** A **single constant** rate is used; the assignment allows **daily varying** rates—production should join a **rates dimension** or API snapshot by **accrual date**.
- **$100 minimum balance:** Gold applies CDI only when **`balance_to_apply_cdi` > 100** (see `gold_silver.py`); thresholds or rules can still be extended (e.g. use `total_amount` instead).
- **Calendar day / payout batching:** Daily boundaries and “pay everyone who qualifies once per day” would add explicit **date** grouping and **partitioning**; the prototype focuses on the **Spark/Delta** skeleton and **documented** extension points from the original notebooks.
- **Tests:** Pytest covers path wiring and core transforms; Delta merge behavior is still best validated with a full local run; see **Tests** above.

---

## License / academic use

Use and extend per your course or employer policy. Keep business logic comments in sync with code when you change rules (CDI rate, thresholds, or time windows).
