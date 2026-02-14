"""
Simple alerting utilities used by pipeline and API.
"""
import os
import logging
from typing import Optional

try:
    from psycopg2 import connect
except ImportError:
    connect = None

logger = logging.getLogger(__name__)


def _post_slack(webhook_url: str, text: str) -> None:
    import json
    import urllib.request
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # nosec
        _ = resp.read()


def send_alert(message: str):
    """Send an alert via Slack if webhook configured, else log."""
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            _post_slack(webhook, message)
        except Exception as e:
            logger.warning(f"Failed to send Slack alert: {e}")
    logger.info(f"ALERT: {message}")


def threshold_price_extractions(db_config: dict, min_count: int = 50):
    """Check raw.price_extractions count in last 24h and alert if above threshold."""
    if connect is None:
        logger.warning("psycopg2 not available, cannot check database")
        return
    try:
        conn = connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM raw.price_extractions WHERE loaded_at >= NOW() - INTERVAL '1 day';"
        )
        count = cur.fetchone()[0]
        conn.close()
        if count >= min_count:
            send_alert(f"High volume of price extractions in last 24h: {count}")
    except Exception as e:
        logger.error(f"Error checking price extractions threshold: {e}")


if __name__ == '__main__':
    # allow standalone invocation
    from dotenv import load_dotenv
    load_dotenv()
    db_cfg = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'medical_warehouse'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres')
    }
    threshold_price_extractions(db_cfg)
