"""
Test PostgreSQL Docker connection
Run this after starting docker-compose
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
import time

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


def test_connection(max_retries=5, delay=2):
    """Test database connection with retries"""
    print("="*60)
    print("TESTING POSTGRESQL DOCKER CONNECTION")
    print("="*60)
    
    print(f"\nConnection details:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User: {DB_CONFIG['user']}")
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n🔄 Attempt {attempt}/{max_retries}...")
            
            # Try to connect
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # Test basic query
            cursor.execute('SELECT version();')
            version = cursor.fetchone()[0]
            
            print(f"\n✅ Successfully connected to PostgreSQL!")
            print(f"\nPostgreSQL Version:")
            print(f"  {version}")
            
            # Check schemas
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('raw', 'staging', 'marts', 'seeds', 'test_failures')
                ORDER BY schema_name;
            """)
            schemas = cursor.fetchall()
            
            print(f"\nSchemas found: {len(schemas)}")
            for schema in schemas:
                print(f"  ✓ {schema[0]}")
            
            # Check if raw.telegram_messages exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw' 
                    AND table_name = 'telegram_messages'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
                count = cursor.fetchone()[0]
                print(f"\n📊 Raw data:")
                print(f"  Messages in raw.telegram_messages: {count:,}")
            else:
                print(f"\n⚠️  Table raw.telegram_messages not yet created")
                print(f"  Run 'python src/load_to_postgres.py' to load data")
            
            # Check database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database()));
            """)
            db_size = cursor.fetchone()[0]
            print(f"\n💾 Database size: {db_size}")
            
            # Clean up
            cursor.close()
            conn.close()
            
            print("\n" + "="*60)
            print("✅ ALL TESTS PASSED!")
            print("="*60)
            print("\nYou can now:")
            print("  1. Load data: python src/load_to_postgres.py")
            print("  2. Access pgAdmin: http://localhost:5050")
            print("  3. Run dbt: cd medical_warehouse && dbt run")
            
            return True
            
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            
            if attempt < max_retries:
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print("\n" + "="*60)
                print("❌ COULD NOT CONNECT TO DATABASE")
                print("="*60)
                print("\nTroubleshooting steps:")
                print("  1. Check if Docker is running:")
                print("     docker ps")
                print("\n  2. Start containers:")
                print("     docker-compose up -d")
                print("\n  3. Check container logs:")
                print("     docker-compose logs postgres")
                print("\n  4. Verify container is healthy:")
                print("     docker-compose ps")
                print("\n  5. Check .env file has correct credentials")
                
                return False
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    return False


if __name__ == '__main__':
    success = test_connection()
    sys.exit(0 if success else 1)