"""
View database content - Medical Telegram Warehouse
"""

import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

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


def view_database():
    """View database content"""
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        
        print("="*80)
        print("MEDICAL TELEGRAM WAREHOUSE - DATABASE CONTENT")
        print("="*80)
        
        # 1. ROW COUNTS
        print("\n📊 ROW COUNTS")
        print("-"*80)
        query = """
        SELECT 'raw.telegram_messages' as layer, COUNT(*) as count 
        FROM raw.telegram_messages
        UNION ALL
        SELECT 'staging (view)', COUNT(*) 
        FROM public_staging.stg_telegram_messages
        UNION ALL
        SELECT 'marts.dim_channels', COUNT(*) 
        FROM public_marts.dim_channels
        UNION ALL
        SELECT 'marts.dim_dates', COUNT(*) 
        FROM public_marts.dim_dates
        UNION ALL
        SELECT 'public.fct_messages', COUNT(*) 
        FROM public.fct_messages;
        """
        df = pd.read_sql(query, conn)
        print(df.to_string(index=False))
        
        # 2. CHANNELS SUMMARY
        print("\n\n📢 CHANNELS SUMMARY")
        print("-"*80)
        query = """
        SELECT 
            channel_name,
            channel_type,
            total_posts,
            ROUND(avg_views::numeric, 0) as avg_views,
            image_content_pct as image_pct,
            first_post_date,
            last_post_date
        FROM public_marts.dim_channels
        ORDER BY total_posts DESC;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No channels found")
        
        # 3. RECENT MESSAGES
        print("\n\n💬 RECENT MESSAGES (Latest 10)")
        print("-"*80)
        query = """
        SELECT 
            c.channel_name,
            d.full_date,
            LEFT(f.message_text, 60) || '...' as message_preview,
            f.view_count,
            f.forward_count,
            f.has_image
        FROM public.fct_messages f
        JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
        JOIN public_marts.dim_dates d ON f.date_key = d.date_key
        ORDER BY f.message_timestamp DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No messages found")
        
        # 4. TOP MESSAGES BY VIEWS
        print("\n\n🔥 TOP MESSAGES BY VIEWS")
        print("-"*80)
        query = """
        SELECT 
            c.channel_name,
            LEFT(f.message_text, 50) || '...' as message,
            f.view_count,
            f.forward_count,
            f.has_image
        FROM public.fct_messages f
        JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
        WHERE f.view_count > 0
        ORDER BY f.view_count DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No messages with views found")
        
        # 5. MESSAGES WITH IMAGES
        print("\n\n🖼️  MESSAGES WITH IMAGES")
        print("-"*80)
        query = """
        SELECT 
            c.channel_name,
            COUNT(*) as image_count,
            ROUND(AVG(f.view_count)::numeric, 0) as avg_views
        FROM public.fct_messages f
        JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
        WHERE f.has_image = true
        GROUP BY c.channel_name
        ORDER BY image_count DESC;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No images found")
        
        # 6. DAILY POSTING TRENDS (Last 14 days)
        print("\n\n📈 DAILY POSTING TRENDS (Last 14 Days)")
        print("-"*80)
        query = """
        SELECT 
            d.full_date,
            d.day_name,
            COUNT(*) as message_count,
            COUNT(CASE WHEN f.has_image THEN 1 END) as with_images,
            ROUND(AVG(f.view_count)::numeric, 0) as avg_views
        FROM public.fct_messages f
        JOIN public_marts.dim_dates d ON f.date_key = d.date_key
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY d.full_date, d.day_name
        ORDER BY d.full_date DESC;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No recent messages found")
        
        # 7. CONTENT TYPE DISTRIBUTION
        print("\n\n📝 CONTENT TYPE DISTRIBUTION")
        print("-"*80)
        query = """
        SELECT 
            content_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
            ROUND(AVG(view_count)::numeric, 0) as avg_views
        FROM public.fct_messages
        GROUP BY content_type
        ORDER BY count DESC;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No content found")
        
        # 8. ENGAGEMENT ANALYSIS
        print("\n\n💡 ENGAGEMENT ANALYSIS")
        print("-"*80)
        query = """
        SELECT 
            engagement_category,
            COUNT(*) as message_count,
            ROUND(AVG(view_count)::numeric, 0) as avg_views,
            ROUND(AVG(forward_count)::numeric, 1) as avg_forwards
        FROM public.fct_messages
        GROUP BY engagement_category
        ORDER BY 
            CASE engagement_category
                WHEN 'High Engagement' THEN 1
                WHEN 'Medium Engagement' THEN 2
                WHEN 'Low Engagement' THEN 3
                WHEN 'No Views' THEN 4
            END;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No engagement data found")
        
        # 9. PRICE MENTIONS
        print("\n\n💰 MESSAGES WITH PRICE MENTIONS")
        print("-"*80)
        query = """
        SELECT 
            c.channel_name,
            COUNT(*) as price_mentions,
            ROUND(COUNT(*) * 100.0 / c.total_posts, 1) as percentage_of_posts
        FROM public.fct_messages f
        JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
        WHERE f.mentions_price = true
        GROUP BY c.channel_name, c.total_posts
        ORDER BY price_mentions DESC;
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No price mentions found")
        
        print("\n" + "="*80)
        print("✅ DATABASE CONTENT DISPLAYED SUCCESSFULLY")
        print("="*80)
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error viewing database: {e}")
        print("\nTroubleshooting:")
        print("1. Check Docker is running: docker-compose ps")
        print("2. Check .env file has correct credentials")
        print("3. Try: python test_docker_connection.py")


if __name__ == '__main__':
    view_database()