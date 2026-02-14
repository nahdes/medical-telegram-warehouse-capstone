"""Database loaders for detection and price results."""

from __future__ import annotations

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import List
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# load env
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'medical_warehouse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

LOGS_DIR = Path('logs')
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f'loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


class YOLOResultsLoader:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = connect_db()
        self.cursor = self.conn.cursor()
        logger.info("Connected to Postgres")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed")

    def create_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS raw.yolo_detections;")
        self.cursor.execute("""
            CREATE TABLE raw.yolo_detections (
                id SERIAL PRIMARY KEY,
                message_id VARCHAR(50),
                channel_name VARCHAR(255),
                image_path VARCHAR(500),
                category VARCHAR(50),
                detected_objects TEXT,
                num_detections INTEGER,
                max_confidence FLOAT,
                detections_json TEXT,
                ocr_text TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.cursor.execute("CREATE INDEX idx_yolo_message_id ON raw.yolo_detections(message_id);")
        self.conn.commit()
        logger.info("YOLO table created")

    def insert_results(self, results: List[dict]):
        if not results:
            logger.warning("No results to insert")
            return
        insert_query = """
            INSERT INTO raw.yolo_detections (
                message_id, channel_name, image_path, category,
                detected_objects, num_detections, max_confidence,
                detections_json, ocr_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = [
            (
                r['message_id'], r['channel_name'], r['image_path'], r['category'],
                r['detected_objects'], int(r['num_detections']), float(r['max_confidence']),
                r['detections_json'], r.get('ocr_text', '')
            )
            for r in results
        ]
        execute_batch(self.cursor, insert_query, batch, page_size=100)
        self.conn.commit()
        logger.info(f"Inserted {len(results)} records")


def load_price_extractions(records: List[dict]):
    loader = PriceResultsLoader()
    loader.connect()
    loader.create_table()
    loader.insert(records)
    loader.disconnect()


class PriceResultsLoader:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = connect_db()
        self.cursor = self.conn.cursor()
        logger.info("Connected to Postgres")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed")

    def create_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS raw.price_extractions;")
        self.cursor.execute("""
            CREATE TABLE raw.price_extractions (
                id SERIAL PRIMARY KEY,
                message_id VARCHAR(50),
                channel_name VARCHAR(255),
                products TEXT,
                prices TEXT,
                source_text TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()
        logger.info("Price extraction table created")

    def insert(self, records: List[dict]):
        if not records:
            return
        insert_q = """
            INSERT INTO raw.price_extractions (
                message_id, channel_name, products, prices, source_text
            ) VALUES (%s, %s, %s, %s, %s)
        """
        batch = [
            (
                r.get('message_id'), r.get('channel_name'),
                ','.join(r.get('products', [])),
                ','.join(r.get('prices', [])),
                r.get('source_text')
            )
            for r in records
        ]
        execute_batch(self.cursor, insert_q, batch, page_size=100)
        self.conn.commit()
        logger.info(f"Inserted {len(records)} price records")
