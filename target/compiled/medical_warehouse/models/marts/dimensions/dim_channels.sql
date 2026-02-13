/*
    Dimension: Channels
    Contains information about each Telegram channel
*/

WITH channel_metrics AS (
    SELECT
        channel_name,
        channel_type,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        COUNT(CASE WHEN has_image THEN 1 END) AS total_posts_with_images,
        AVG(view_count) AS avg_views,
        AVG(forward_count) AS avg_forwards,
        MAX(view_count) AS max_views,
        SUM(view_count) AS total_views,
        SUM(forward_count) AS total_forwards,
        AVG(message_length) AS avg_message_length,
        
        -- Engagement rate (posts with > 0 views)
        COUNT(CASE WHEN view_count > 0 THEN 1 END)::FLOAT / 
            NULLIF(COUNT(*), 0) AS engagement_rate,
        
        -- Image content ratio
        COUNT(CASE WHEN has_image THEN 1 END)::FLOAT / 
            NULLIF(COUNT(*), 0) AS image_content_ratio

    FROM "medical_warehouse"."public_staging"."stg_telegram_messages"
    GROUP BY channel_name, channel_type
)

SELECT
    -- Surrogate key
    md5(cast(coalesce(cast(channel_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS channel_key,
    
    -- Natural key and attributes
    channel_name,
    channel_type,
    
    -- Time metrics
    first_post_date,
    last_post_date,
    (last_post_date - first_post_date) AS days_active,
    
    -- Volume metrics
    total_posts,
    total_posts_with_images,
    ROUND(
        total_posts::NUMERIC / NULLIF((last_post_date - first_post_date), 0), 
        2
    ) AS avg_posts_per_day,
    
    -- Engagement metrics
    ROUND(avg_views::NUMERIC, 2) AS avg_views,
    ROUND(avg_forwards::NUMERIC, 2) AS avg_forwards,
    max_views,
    total_views,
    total_forwards,
    ROUND(avg_message_length::NUMERIC, 2) AS avg_message_length,
    
    -- Derived metrics
    ROUND(engagement_rate::NUMERIC * 100, 2) AS engagement_rate_pct,
    ROUND(image_content_ratio::NUMERIC * 100, 2) AS image_content_pct,
    
    -- Metadata
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM channel_metrics