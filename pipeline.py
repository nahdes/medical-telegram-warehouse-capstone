from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Optional

from dagster import (
    Definitions,
    Failure,
    HookContext,
    OpExecutionContext,
    ScheduleDefinition,
    failure_hook,
    job,
    op,
)


def _run_cmd(
    *,
    context: OpExecutionContext,
    cmd: list[str],
    cwd: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
):
    context.log.info(f"Running: {' '.join(cmd)}")
    if cwd:
        context.log.info(f"cwd: {cwd}")

    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    # Stream output live into Dagster logs
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        context.log.info(line.rstrip("\n"))

    rc = proc.wait()
    if rc != 0:
        raise Failure(f"Command failed (exit={rc}): {' '.join(cmd)}")


@dataclass
class SlackAlertConfig:
    # Optional: set this to enable Slack alerts (Incoming Webhook URL)
    webhook_url: Optional[str] = None


def _post_slack(webhook_url: str, text: str) -> None:
    # Avoid adding new deps (requests) by using stdlib.
    import json
    import urllib.request

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # nosec - user-provided URL
        _ = resp.read()


@failure_hook(required_resource_keys=set())
def notify_on_failure(context: HookContext):
    """
    Basic failure alerting:
    - Always logs a clear message in Dagster
    - Optionally posts to Slack if SLACK_WEBHOOK_URL is set
    """
    run_id = context.run_id
    job_name = context.job_name
    op_name = context.op.name if context.op else "unknown_op"
    err = context.failure_event.message if context.failure_event else "unknown failure"

    msg = f"Dagster run failed: job={job_name} op={op_name} run_id={run_id}\n{err}"
    context.log.error(msg)

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            _post_slack(webhook, msg)
        except Exception as e:
            context.log.warning(f"Failed to send Slack alert: {e}")


@op
def scrape_telegram_data(context: OpExecutionContext):
    """
    Runs your Telegram scraping step.

    Note: `src/scraper.py` is currently empty in this repo. This op will fail
    until the scraper is implemented (or you swap it to the correct script).
    """
    _run_cmd(context=context, cmd=["python", "src/scraper.py"])


@op
def load_raw_to_postgres(context: OpExecutionContext):
    """Loads JSON partitions from `data/raw/telegram_messages/**` into Postgres raw schema."""
    _run_cmd(context=context, cmd=["python", "src/load_to_postgres.py"])


@op
def run_dbt_transformations(context: OpExecutionContext):
    """
    Executes dbt models (repo-root dbt project).

    Expects dbt installed and `profile.yml` working, plus env vars like DB_PASSWORD.
    """
    project_dir = os.getcwd()
    _run_cmd(context=context, cmd=["dbt", "deps"], cwd=project_dir)
    _run_cmd(context=context, cmd=["dbt", "run"], cwd=project_dir)


@op
def run_yolo_enrichment(context: OpExecutionContext):
    """
    Runs YOLO detection, producing `data/processed/yolo_detections.csv`,
    then loads results into Postgres (`raw.yolo_detections`).
    """
    _run_cmd(context=context, cmd=["python", "src/yolo_detect.py"])
    _run_cmd(context=context, cmd=["python", "src/load_yolo_results.py"])


@job(hooks={notify_on_failure})
def medical_telegram_warehouse_job():
    # Order:
    # 1) scrape -> 2) raw load -> 3) dbt -> 4) yolo + load yolo
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()


# Daily schedule (default: 02:00 local time)
daily_schedule = ScheduleDefinition(
    job=medical_telegram_warehouse_job,
    cron_schedule="0 2 * * *",
    name="daily_medical_telegram_warehouse",
)


defs = Definitions(
    jobs=[medical_telegram_warehouse_job],
    schedules=[daily_schedule],
)

