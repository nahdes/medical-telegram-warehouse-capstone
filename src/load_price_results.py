"""
Load product/price extraction results into PostgreSQL
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load env
load_dotenv()

# Database configuration
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
        logging.FileHandler(LOGS_DIR / f'load_prices_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PriceResultsLoader:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

    def create_table(self):
        """Create raw.price_extractions"""
        try:
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
            self.cursor.execute("CREATE INDEX idx_price_message ON raw.price_extractions(message_id);")
            self.conn.commit()
            logger.info("Table raw.price_extractions created successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating table: {e}")
            raise

    def insert(self, records: list):
        if not records:
            logger.warning("No price records to insert")
            return
        try:
            insert_q = """
                INSERT INTO raw.price_extractions (
                    message_id, channel_name, products, prices, source_text
                ) VALUES (%s, %s, %s, %s, %s)
            """
            batch = [
                (
                    r.get('message_id'),
                    r.get('channel_name'),
                    ','.join(r.get('products', [])),
                    ','.join(r.get('prices', [])),
                    r.get('source_text')
                )
                for r in records
            ]
            execute_batch(self.cursor, insert_q, batch, page_size=100)
            self.conn.commit()
            logger.info(f"Inserted {len(records)} price extraction records")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting records: {e}")
            raise


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python src/load_price_results.py <csv file>")
        sys.exit(1)
    file = sys.argv[1]
    from pipeline.transform import process_yolo_ocr_file
    recs = process_yolo_ocr_file(file)
    loader = PriceResultsLoader(DB_CONFIG)
    loader.connect()
    loader.create_table()
    loader.insert(recs)
    loader.get_statistics = lambda: None  # no stats for now
    loader.disconnect()
