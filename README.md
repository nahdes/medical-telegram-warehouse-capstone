## Medical Telegram Warehouse

End-to-end analytics pipeline for Ethiopian medical/pharmaceutical Telegram channels.  
It scrapes messages and images, loads them into PostgreSQL, transforms them with dbt into a star schema, enriches images with YOLO object detection, and exposes insights via a FastAPI API and a Dagster-orchestrated pipeline.

---

## Architecture Overview

- **Extract**: (intended) Telegram scraper writes JSON to `data/raw/telegram_messages/YYYY-MM-DD/*.json` and images to `data/raw/images/<channel>/*.jpg`.
- **Load (raw)**: `src/load_to_postgres.py` loads JSON into `raw.telegram_messages` in PostgreSQL.
- **Transform (dbt)**:
  - `models/staging/stg_telegram_messages.sql`: cleans and standardizes raw messages.
  - `models/marts/dimensions/dim_channels.sql`, `models/marts/dimensions/dim_dates.sql`: build core dimensions.
  - `models/core/fct_messages.sql`: core fact table with engagement and text flags.
- **Enrich (YOLO)**:
  - `src/yolo_detect.py`: runs YOLOv8 on images, writing `data/processed/yolo_detections.csv`.
  - `src/load_yolo_results.py`: loads CSV into `raw.yolo_detections`.
  - `models/marts/core/fct_image_detections.sql`: combines YOLO results with message facts.
- **Serve (API)**: `api/main.py` exposes REST endpoints for analytics using SQLAlchemy + Pydantic.
- **Orchestrate (Dagster)**: `pipeline.py` defines a Dagster job to run the whole pipeline on a schedule.

---

## Project Layout (high level)

- `src/`
  - `scraper.py` – **placeholder** for Telegram scraping (currently empty in this repo).
  - `load_to_postgres.py` – load raw JSON → `raw.telegram_messages`.
  - `yolo_detect.py` – run YOLO over `data/raw/images/**`, output CSV.
  - `load_yolo_results.py` – load YOLO CSV → `raw.yolo_detections`.
  - `api/` – FastAPI app (`main.py`) + SQLAlchemy setup + query functions + Pydantic schemas.
- `models/`
  - `staging/stg_telegram_messages.sql` – standardized staging layer.
  - `core/fct_messages.sql` – main message fact table.
  - `marts/dimensions/dim_channels.sql`, `marts/dimensions/dim_dates.sql` – dimensions.
  - `marts/core/fct_image_detections.sql` – YOLO-enriched fact table.
  - `marts/schema.yml` – dbt tests and documentation.
- `dbt_project.yml` – dbt project config (project root).
- `profile.yml` – dbt profiles (dev/prod outputs).
- `packages.yml` – dbt packages (`dbt_utils`).
- `docker-compose.yml` – Postgres + optional pgAdmin, wired to `setup_database.sql`.
- `data/raw/` – raw Telegram messages and images.
- `data/processed/` – processed outputs (e.g., YOLO detections CSV).
- `pipeline.py` – Dagster job + schedule + failure hooks.

---

## Local Setup

### 1. Clone and create virtual environment

```bash
git clone <this-repo>
cd medical-telegram-warehouse
python -m venv venv
venv\Scripts\activate  # on Windows
pip install -r requirements.txt
```

### 2. Start PostgreSQL via Docker

```bash
docker-compose up -d
```

This:
- Starts a `postgres:15-alpine` container.
- Initializes schemas (`raw`, `staging`, `marts`, `seeds`, `test_failures`) from `setup_database.sql`.

You can override DB settings with env vars (examples):

```bash
set DB_USER=postgres
set DB_PASSWORD=postgres
set DB_NAME=medical_warehouse
set DB_PORT=5432
```

### 3. Configure dbt

`profile.yml` is checked into the repo and expects:

- `DB_PASSWORD` env var for the `dev` target.
- Postgres reachable at `localhost:5432` with database `medical_warehouse`.

Basic commands:

```bash
dbt deps
dbt run
dbt test
```

---

## Pipeline Steps (Manual)

**Prerequisite**: you have already scraped data (JSON + images) into:
- `data/raw/telegram_messages/YYYY-MM-DD/*.json`
- `data/raw/images/<channel>/*.jpg`

1. **Load raw JSON to Postgres**

```bash
python src/load_to_postgres.py
```

This populates `raw.telegram_messages`.

2. **Run dbt transformations**

```bash
dbt deps
dbt run
dbt test
```

This builds staging + marts, including:
- `stg_telegram_messages`
- `dim_channels`, `dim_dates`
- `fct_messages`

3. **Run YOLO detection**

```bash
python src/yolo_detect.py
```

This:
- Scans `data/raw/images/<channel>/*.jpg`.
- Runs YOLOv8 (nano model) on each image.
- Writes results to `data/processed/yolo_detections.csv`.

4. **Load YOLO detections into Postgres**

```bash
python src/load_yolo_results.py
```

This populates `raw.yolo_detections`.

5. **(Optional) Re-run dbt to materialize YOLO marts**

```bash
dbt run --select fct_image_detections
```

---

## Orchestration with Dagster

The repo includes a Dagster job to automate the pipeline.

### Dagster components (`pipeline.py`)

- **Ops**
  - `scrape_telegram_data` – runs `python src/scraper.py`  
    - Note: `src/scraper.py` is currently a placeholder; implement your scraper or adjust this command.
  - `load_raw_to_postgres` – runs `python src/load_to_postgres.py`.
  - `run_dbt_transformations` – runs `dbt deps` and `dbt run` from the project root.
  - `run_yolo_enrichment` – runs `python src/yolo_detect.py` and `python src/load_yolo_results.py`.

- **Job graph**
  - `medical_telegram_warehouse_job` executes ops in order:
    1. Scrape Telegram data.
    2. Load raw JSON to Postgres.
    3. Run dbt transformations.
    4. Run YOLO detection + load YOLO results.

- **Scheduling**
  - `daily_medical_telegram_warehouse` schedule:
    - Cron: `0 2 * * *` (runs daily at 02:00 local time).

- **Failure alerts**
  - `notify_on_failure` hook:
    - Logs failures with job, op, and run ID.
    - Optionally posts to Slack if `SLACK_WEBHOOK_URL` env var is set (Incoming Webhook URL).

### Running Dagster locally

1. Install Dagster (if not already):

```bash
pip install dagster dagster-webserver
```

2. Start Dagster:

```bash
dagster dev -f pipeline.py
```

3. Open `http://localhost:3000`:
   - Locate and run `medical_telegram_warehouse_job`.
   - Monitor step-by-step logs.

> For course deliverables: capture screenshots of the Dagster UI showing a successful run (Runs list + Run detail view).

---

## FastAPI Analytics API

The `api/` package exposes analytics endpoints on top of the warehouse.

- `api/main.py`:
  - `/` and `/health` – service and DB health checks.
  - `/api/reports/top-products` – top terms/products via simple word frequency over `fct_messages`.
  - `/api/channels` – list of channels with basic stats.
  - `/api/channels/{channel_name}/activity` – detailed metrics per channel (using `dim_channels`).
  - `/api/search/messages` – message search by keyword/channel.
  - `/api/reports/visual-content` – YOLO + image statistics.
  - `/api/reports/image-performance` – engagement metrics by image category.

- `api/database.py`:
  - SQLAlchemy engine + session management using env vars `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

### Running the API

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Then open:
- API docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`

---

## Data Quality & Testing

- **dbt tests** via `models/marts/schema.yml`:
  - `unique`, `not_null`, `accepted_values`, and `relationships` tests for dimensions and facts.
- **Custom SQL tests** in `tests/`:
  - `assert_no_orphan_facts.sql` – ensures every fact record has valid dimension references (`dim_channels`, `dim_dates`).
  - Additional constraints on views, future dates, etc.

Run:

```bash
dbt test
```

---

## Known Gaps / Notes

- **Scraper not implemented in this repo**:
  - `src/scraper.py` is currently empty; implement your own Telethon-based scraper or wire the Dagster `scrape_telegram_data` op to the correct script.
- **Destructive loads**:
  - Raw loaders drop and recreate `raw.telegram_messages` and `raw.yolo_detections` on each run; this is fine for a learning project but not ideal for production/incremental history.
- **Schema alignment**:
  - Ensure the dbt target schemas match what the API queries expect (e.g., `public_marts.dim_channels` vs your actual schema naming).

---

## Quickstart Summary

1. Start Postgres with `docker-compose up -d`.
2. Ensure raw JSON/images exist in `data/raw/...`.
3. `python src/load_to_postgres.py`.
4. `dbt deps && dbt run && dbt test`.
5. `python src/yolo_detect.py` then `python src/load_yolo_results.py` (and optionally `dbt run --select fct_image_detections`).
6. Optionally orchestrate via Dagster: `dagster dev -f pipeline.py` and run `medical_telegram_warehouse_job`.
7. Optionally start the FastAPI API with `uvicorn api.main:app --reload`.

