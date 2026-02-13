"""
Database query functions for API endpoints
"""

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from api.schemas import (
    TopProduct,
    ChannelActivity,
    MessageSearchResult,
    VisualContentStats,
    ImageCategoryBreakdown,
    ChannelImageStats,
    ChannelStats,
    ImageCategoryStats
)


def get_top_products(db: Session, limit: int = 10) -> List[TopProduct]:
    """
    Get most frequently mentioned terms/products
    Uses simple word frequency analysis
    """
    query = text("""
        WITH words AS (
            SELECT 
                LOWER(TRIM(word)) as term,
                view_count
            FROM public.fct_messages,
            LATERAL UNNEST(STRING_TO_ARRAY(message_text, ' ')) as word
            WHERE LENGTH(TRIM(word)) > 3  -- Skip short words
                AND message_text IS NOT NULL
                AND message_text != ''
        ),
        word_stats AS (
            SELECT 
                term,
                COUNT(*) as frequency,
                ROUND(AVG(view_count)::NUMERIC, 1) as avg_views,
                COUNT(DISTINCT view_count) as unique_messages
            FROM words
            GROUP BY term
            HAVING COUNT(*) > 2  -- Must appear at least 3 times
        )
        SELECT 
            term,
            frequency::INTEGER,
            avg_views::FLOAT,
            unique_messages::INTEGER as channels
        FROM word_stats
        ORDER BY frequency DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {"limit": limit})
    
    return [
        TopProduct(
            term=row[0],
            frequency=row[1],
            avg_views=row[2],
            channels=row[3]
        )
        for row in result
    ]


def get_channel_activity(db: Session, channel_name: str) -> Optional[ChannelActivity]:
    """Get activity metrics for a specific channel"""
    query = text("""
        SELECT 
            channel_name,
            channel_type,
            total_posts,
            first_post_date,
            last_post_date,
            days_active,
            avg_posts_per_day,
            avg_views,
            avg_forwards,
            total_views,
            image_content_pct
        FROM public_marts.dim_channels
        WHERE LOWER(channel_name) = LOWER(:channel_name)
    """)
    
    result = db.execute(query, {"channel_name": channel_name}).fetchone()
    
    if not result:
        return None
    
    return ChannelActivity(
        channel_name=result[0],
        channel_type=result[1],
        total_posts=result[2],
        first_post_date=result[3],
        last_post_date=result[4],
        days_active=result[5],
        avg_posts_per_day=float(result[6]) if result[6] else None,
        avg_views=float(result[7]) if result[7] else None,
        avg_forwards=float(result[8]) if result[8] else None,
        total_views=result[9],
        image_content_pct=float(result[10]) if result[10] else None
    )


def get_all_channels(db: Session) -> List[ChannelStats]:
    """Get list of all channels with basic stats"""
    query = text("""
        SELECT 
            channel_name,
            channel_type,
            total_posts,
            avg_views,
            image_content_pct
        FROM public_marts.dim_channels
        ORDER BY total_posts DESC
    """)
    
    result = db.execute(query)
    
    return [
        ChannelStats(
            channel_name=row[0],
            channel_type=row[1],
            total_posts=row[2],
            avg_views=float(row[3]) if row[3] else None,
            image_content_pct=float(row[4]) if row[4] else None
        )
        for row in result
    ]


def search_messages(
    db: Session,
    query_text: str,
    channel: Optional[str] = None,
    limit: int = 20
) -> List[MessageSearchResult]:
    """Search messages by keyword"""
    
    # Base query
    sql = """
        SELECT 
            f.message_id,
            c.channel_name,
            f.message_timestamp,
            LEFT(f.message_text, 200) as message_text,
            f.view_count,
            f.forward_count,
            f.has_image
        FROM public.fct_messages f
        JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
        WHERE LOWER(f.message_text) LIKE LOWER(:search_pattern)
    """
    
    # Add channel filter if specified
    if channel:
        sql += " AND LOWER(c.channel_name) = LOWER(:channel)"
    
    sql += " ORDER BY f.view_count DESC, f.message_timestamp DESC LIMIT :limit"
    
    params = {
        "search_pattern": f"%{query_text}%",
        "limit": limit
    }
    if channel:
        params["channel"] = channel
    
    result = db.execute(text(sql), params)
    
    return [
        MessageSearchResult(
            message_id=row[0],
            channel_name=row[1],
            message_date=row[2],
            message_text=row[3] or "",
            view_count=row[4] or 0,
            forward_count=row[5] or 0,
            has_image=row[6] or False
        )
        for row in result
    ]


def get_visual_content_stats(db: Session) -> VisualContentStats:
    """Get comprehensive visual content statistics"""
    
    # Overall stats
    overall_query = text("""
        SELECT 
            COUNT(*) as total_images,
            COUNT(CASE WHEN num_detections > 0 THEN 1 END) as with_detections,
            ROUND(AVG(num_detections)::NUMERIC, 1) as avg_objects,
            ROUND(AVG(detection_confidence)::NUMERIC, 3) as avg_confidence
        FROM public.fct_image_detections
    """)
    
    overall = db.execute(overall_query).fetchone()
    
    # Category breakdown
    category_query = text("""
        SELECT 
            image_category,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
            ROUND(AVG(view_count)::NUMERIC, 1) as avg_views,
            ROUND(AVG(detection_confidence)::NUMERIC, 3) as avg_confidence
        FROM public.fct_image_detections
        GROUP BY image_category
        ORDER BY count DESC
    """)
    
    categories = db.execute(category_query)
    category_breakdown = [
        ImageCategoryBreakdown(
            category=row[0],
            count=row[1],
            percentage=float(row[2]),
            avg_views=float(row[3]) if row[3] else None,
            avg_confidence=float(row[4]) if row[4] else None
        )
        for row in categories
    ]
    
    # Channel breakdown
    channel_query = text("""
        SELECT 
            c.channel_name,
            COUNT(*) as total_images,
            ROUND(AVG(d.num_detections)::NUMERIC, 1) as avg_detections,
            COUNT(CASE WHEN d.is_promotional THEN 1 END) as promotional_count,
            COUNT(CASE WHEN d.is_product_only THEN 1 END) as product_display_count
        FROM public.fct_image_detections d
        JOIN public_marts.dim_channels c ON d.channel_key = c.channel_key
        GROUP BY c.channel_name
        ORDER BY total_images DESC
    """)
    
    channels = db.execute(channel_query)
    channel_breakdown = [
        ChannelImageStats(
            channel_name=row[0],
            total_images=row[1],
            avg_detections=float(row[2]) if row[2] else None,
            promotional_count=row[3],
            product_display_count=row[4]
        )
        for row in channels
    ]
    
    return VisualContentStats(
        total_images=overall[0] or 0,
        images_with_detections=overall[1] or 0,
        avg_objects_per_image=float(overall[2]) if overall[2] else None,
        avg_detection_confidence=float(overall[3]) if overall[3] else None,
        category_breakdown=category_breakdown,
        channel_breakdown=channel_breakdown
    )


def get_image_category_performance(db: Session) -> List[ImageCategoryStats]:
    """Compare performance metrics across image categories"""
    query = text("""
        SELECT 
            image_category,
            COUNT(*) as count,
            ROUND(AVG(view_count)::NUMERIC, 1) as avg_views,
            ROUND(AVG(forward_count)::NUMERIC, 1) as avg_forwards,
            MAX(view_count) as max_views,
            SUM(view_count) as total_views
        FROM public.fct_image_detections
        GROUP BY image_category
        ORDER BY avg_views DESC
    """)
    
    result = db.execute(query)
    
    return [
        ImageCategoryStats(
            category=row[0],
            count=row[1],
            avg_views=float(row[2]) if row[2] else 0.0,
            avg_forwards=float(row[3]) if row[3] else 0.0,
            max_views=row[4] or 0,
            total_views=row[5] or 0
        )
        for row in result
    ]