"""
Check what actually exists in the database
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'medical_warehouse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("="*80)
    print("DATABASE STRUCTURE DIAGNOSIS")
    print("="*80)
    
    # 1. Check all schemas
    print("\n📁 SCHEMAS:")
    print("-"*80)
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name;
    """)
    schemas = cursor.fetchall()
    for schema in schemas:
        print(f"  ✓ {schema[0]}")
    
    # 2. Check all tables in each schema
    print("\n\n📊 TABLES BY SCHEMA:")
    print("-"*80)
    for schema in schemas:
        schema_name = schema[0]
        cursor.execute(f"""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        if tables:
            print(f"\n{schema_name}:")
            for table, table_type in tables:
                icon = "📋" if table_type == "BASE TABLE" else "👁️"
                print(f"  {icon} {table} ({table_type})")
        else:
            print(f"\n{schema_name}: (empty)")
    
    # 3. Check raw data
    print("\n\n📦 RAW DATA CHECK:")
    print("-"*80)
    try:
        cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
        count = cursor.fetchone()[0]
        print(f"✓ raw.telegram_messages: {count:,} rows")
        
        if count > 0:
            cursor.execute("SELECT channel_name, COUNT(*) FROM raw.telegram_messages GROUP BY channel_name;")
            channels = cursor.fetchall()
            print("\nMessages by channel:")
            for channel, cnt in channels:
                print(f"  - {channel}: {cnt}")
    except Exception as e:
        print(f"✗ raw.telegram_messages: {e}")
    
    # 4. Check if dbt models exist
    print("\n\n🔄 DBT MODELS CHECK:")
    print("-"*80)
    
    # Try different possible schema names
    possible_schemas = [
        ('staging', 'stg_telegram_messages'),
        ('public_staging', 'stg_telegram_messages'),
        ('marts', 'dim_channels'),
        ('public_marts', 'dim_channels'),
        ('marts', 'fct_messages'),
        ('public_marts', 'fct_messages'),
        ('public', 'dim_channels'),
        ('public', 'fct_messages'),
    ]
    
    found_models = []
    for schema, table in possible_schemas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            count = cursor.fetchone()[0]
            print(f"✓ {schema}.{table}: {count:,} rows")
            found_models.append((schema, table, count))
        except:
            pass
    
    if not found_models:
        print("⚠️  No dbt models found!")
        print("\nYou need to run:")
        print("  cd medical_warehouse")
        print("  dbt run")
    
    print("\n" + "="*80)
    print("✅ DIAGNOSIS COMPLETE")
    print("="*80)
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n❌ Error: {e}")