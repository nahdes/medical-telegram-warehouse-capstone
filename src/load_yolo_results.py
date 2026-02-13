"""
Load YOLO detection results into PostgreSQL
"""

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'medical_warehouse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Paths
YOLO_RESULTS = Path('data/processed/yolo_detections.csv')
LOGS_DIR = Path('logs')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f'load_yolo_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class YOLOResultsLoader:
    """Load YOLO detection results into PostgreSQL"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def create_table(self):
        """Create table for YOLO results"""
        try:
            # Drop existing table
            self.cursor.execute("DROP TABLE IF EXISTS raw.yolo_detections;")
            
            # Create new table
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
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes
            self.cursor.execute("""
                CREATE INDEX idx_yolo_message_id ON raw.yolo_detections(message_id);
                CREATE INDEX idx_yolo_channel ON raw.yolo_detections(channel_name);
                CREATE INDEX idx_yolo_category ON raw.yolo_detections(category);
            """)
            
            self.conn.commit()
            logger.info("Table raw.yolo_detections created successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating table: {e}")
            raise
    
    def load_csv(self) -> list:
        """Load YOLO results from CSV"""
        if not YOLO_RESULTS.exists():
            logger.error(f"YOLO results file not found: {YOLO_RESULTS}")
            raise FileNotFoundError(f"Run YOLO detection first: python src/yolo_detect.py")
        
        results = []
        try:
            with open(YOLO_RESULTS, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                results = list(reader)
            
            logger.info(f"Loaded {len(results)} records from CSV")
            return results
            
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise
    
    def insert_results(self, results: list):
        """Insert results into database"""
        if not results:
            logger.warning("No results to insert")
            return
        
        try:
            insert_query = """
                INSERT INTO raw.yolo_detections (
                    message_id, channel_name, image_path, category,
                    detected_objects, num_detections, max_confidence,
                    detections_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            batch_data = [
                (
                    r['message_id'],
                    r['channel_name'],
                    r['image_path'],
                    r['category'],
                    r['detected_objects'],
                    int(r['num_detections']),
                    float(r['max_confidence']),
                    r['detections_json']
                )
                for r in results
            ]
            
            execute_batch(self.cursor, insert_query, batch_data, page_size=100)
            self.conn.commit()
            
            logger.info(f"Successfully inserted {len(results)} records")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting results: {e}")
            raise
    
    def get_statistics(self):
        """Print statistics"""
        try:
            # Total records
            self.cursor.execute("SELECT COUNT(*) FROM raw.yolo_detections")
            total = self.cursor.fetchone()[0]
            
            # Category distribution
            self.cursor.execute("""
                SELECT category, COUNT(*) as count
                FROM raw.yolo_detections
                GROUP BY category
                ORDER BY count DESC
            """)
            categories = self.cursor.fetchall()
            
            # Channel distribution
            self.cursor.execute("""
                SELECT channel_name, COUNT(*) as count
                FROM raw.yolo_detections
                GROUP BY channel_name
                ORDER BY count DESC
            """)
            channels = self.cursor.fetchall()
            
            print("\n" + "="*60)
            print("YOLO RESULTS LOADED TO DATABASE")
            print("="*60)
            print(f"\nTotal Records: {total:,}")
            
            print(f"\nCategory Distribution:")
            for cat, count in categories:
                pct = (count / total) * 100
                print(f"  {cat:20} {count:4} ({pct:5.1f}%)")
            
            print(f"\nImages by Channel:")
            for ch, count in channels:
                print(f"  {ch:20} {count:4}")
            
            print("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
    
    def run(self):
        """Main execution"""
        try:
            self.connect()
            self.create_table()
            results = self.load_csv()
            self.insert_results(results)
            self.get_statistics()
            
        except Exception as e:
            logger.error(f"Error in execution: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Entry point"""
    print("Loading YOLO results into PostgreSQL...")
    
    loader = YOLOResultsLoader(DB_CONFIG)
    loader.run()
    
    print("\n✅ YOLO results loaded successfully!")
    print("   Next: Run dbt to create fct_image_detections model")


if __name__ == '__main__':
    main()