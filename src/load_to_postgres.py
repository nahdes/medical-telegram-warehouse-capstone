"""
Load raw JSON data from data lake to PostgreSQL
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict
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
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Paths
MESSAGES_DIR = Path('data/raw/telegram_messages')
LOGS_DIR = Path('logs')

# Setup logging
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f'data_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Load raw data from JSON files into PostgreSQL"""
    
    def __init__(self, db_config: Dict):
        """Initialize database connection"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def create_schema_and_table(self):
        """Create raw schema and telegram_messages table"""
        try:
            # Create raw schema if not exists
            self.cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
            """)
            
            # Drop existing table (for development)
            self.cursor.execute("""
                DROP TABLE IF EXISTS raw.telegram_messages;
            """)
            
            # Create raw.telegram_messages table
            self.cursor.execute("""
                CREATE TABLE raw.telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    channel_name VARCHAR(255),
                    message_date TIMESTAMP,
                    message_text TEXT,
                    has_media BOOLEAN,
                    image_path VARCHAR(500),
                    views INTEGER,
                    forwards INTEGER,
                    is_reply BOOLEAN,
                    reply_to_msg_id BIGINT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_file VARCHAR(500)
                );
            """)
            
            # Create indexes for better query performance
            self.cursor.execute("""
                CREATE INDEX idx_channel_name ON raw.telegram_messages(channel_name);
                CREATE INDEX idx_message_date ON raw.telegram_messages(message_date);
                CREATE INDEX idx_has_media ON raw.telegram_messages(has_media);
            """)
            
            self.conn.commit()
            logger.info("Schema and table created successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating schema/table: {str(e)}")
            raise
    
    def load_json_files(self) -> List[Dict]:
        """Read all JSON files from data lake"""
        all_messages = []
        
        if not MESSAGES_DIR.exists():
            logger.warning(f"Messages directory not found: {MESSAGES_DIR}")
            return all_messages
        
        # Iterate through date partitions
        for date_dir in MESSAGES_DIR.iterdir():
            if not date_dir.is_dir():
                continue
            
            logger.info(f"Processing partition: {date_dir.name}")
            
            # Read all JSON files in this partition
            for json_file in date_dir.glob('*.json'):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        messages = json.load(f)
                    
                    # Add source file information
                    for msg in messages:
                        msg['source_file'] = str(json_file.relative_to(Path('.')))
                    
                    all_messages.extend(messages)
                    logger.info(f"Loaded {len(messages)} messages from {json_file.name}")
                    
                except Exception as e:
                    logger.error(f"Error reading {json_file}: {str(e)}")
                    continue
        
        logger.info(f"Total messages loaded: {len(all_messages)}")
        return all_messages
    
    def insert_messages(self, messages: List[Dict]):
        """Insert messages into PostgreSQL using batch insert"""
        if not messages:
            logger.warning("No messages to insert")
            return
        
        try:
            # Prepare data for insertion
            insert_query = """
                INSERT INTO raw.telegram_messages (
                    message_id, channel_name, message_date, message_text,
                    has_media, image_path, views, forwards,
                    is_reply, reply_to_msg_id, source_file
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            # Prepare batch data
            batch_data = []
            for msg in messages:
                batch_data.append((
                    msg.get('message_id'),
                    msg.get('channel_name'),
                    msg.get('message_date'),
                    msg.get('message_text'),
                    msg.get('has_media', False),
                    msg.get('image_path'),
                    msg.get('views', 0),
                    msg.get('forwards', 0),
                    msg.get('is_reply', False),
                    msg.get('reply_to_msg_id'),
                    msg.get('source_file')
                ))
            
            # Execute batch insert
            execute_batch(self.cursor, insert_query, batch_data, page_size=1000)
            self.conn.commit()
            
            logger.info(f"Successfully inserted {len(messages)} messages")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting messages: {str(e)}")
            raise
    
    def get_statistics(self):
        """Get basic statistics about loaded data"""
        try:
            # Total messages
            self.cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages")
            total_count = self.cursor.fetchone()[0]
            
            # Messages by channel
            self.cursor.execute("""
                SELECT channel_name, COUNT(*) as count
                FROM raw.telegram_messages
                GROUP BY channel_name
                ORDER BY count DESC
            """)
            channel_stats = self.cursor.fetchall()
            
            # Date range
            self.cursor.execute("""
                SELECT MIN(message_date), MAX(message_date)
                FROM raw.telegram_messages
            """)
            date_range = self.cursor.fetchone()
            
            # Messages with media
            self.cursor.execute("""
                SELECT COUNT(*) FROM raw.telegram_messages WHERE has_media = TRUE
            """)
            media_count = self.cursor.fetchone()[0]
            
            # Print statistics
            print("\n" + "="*60)
            print("DATA LOADING STATISTICS")
            print("="*60)
            print(f"Total Messages: {total_count:,}")
            print(f"Messages with Media: {media_count:,}")
            print(f"\nDate Range: {date_range[0]} to {date_range[1]}")
            print(f"\nMessages by Channel:")
            for channel, count in channel_stats:
                print(f"  - {channel}: {count:,}")
            print("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
    
    def run(self):
        """Main execution method"""
        try:
            # Connect to database
            self.connect()
            
            # Create schema and table
            self.create_schema_and_table()
            
            # Load JSON files
            messages = self.load_json_files()
            
            # Insert into PostgreSQL
            if messages:
                self.insert_messages(messages)
                
                # Show statistics
                self.get_statistics()
            else:
                logger.warning("No messages found to load")
            
        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
            raise
        finally:
            self.disconnect()


def main():
    """Entry point"""
    print("Starting data loading process...")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"User: {DB_CONFIG['user']}")
    
    loader = PostgreSQLLoader(DB_CONFIG)
    loader.run()
    
    print("\n✅ Data loading completed successfully!")


if __name__ == '__main__':
    main()