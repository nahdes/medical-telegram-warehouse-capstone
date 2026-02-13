/*
    Fact Table: Messages
    One row per message with foreign keys to dimension tables
*/

WITH messages AS (
    SELECT * FROM "medical_warehouse"."public_staging"."stg_telegram_messages"
),

channels AS (
    SELECT 
        channel_key,
        channel_name
    FROM "medical_warehouse"."public_marts"."dim_channels"
),

dates AS (
    SELECT
        date_key,
        full_date
    FROM "medical_warehouse"."public_marts"."dim_dates"
)

SELECT
    -- Fact table primary key
    md5(cast(coalesce(cast(m.message_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(m.channel_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS message_key,
    
    -- Natural key
    m.message_id,
    
    -- Foreign keys to dimensions
    c.channel_key,
    d.date_key,
    
    -- Degenerate dimensions (attributes that don't warrant their own dimension)
    m.message_timestamp,
    m.message_hour,
    m.day_of_week,
    
    -- Message content
    m.message_text,
    m.message_length,
    m.is_empty_message,
    
    -- Media attributes
    m.has_media,
    m.has_image,
    m.image_path,
    
    -- Engagement metrics (measures/facts)
    m.view_count,
    m.forward_count,
    
    -- Calculated engagement metrics
    CASE 
        WHEN m.view_count > 0 THEN m.forward_count::FLOAT / m.view_count
        ELSE 0
    END AS forward_rate,
    
    -- Reply context
    m.is_reply,
    m.reply_to_msg_id,
    
    -- Text analysis flags
    CASE 
        WHEN LOWER(m.message_text) LIKE '%price%' 
          OR LOWER(m.message_text) LIKE '%birr%'
          OR LOWER(m.message_text) LIKE '%br%'
          OR m.message_text ~ '\d+\s*(birr|br|etb)' THEN TRUE
        ELSE FALSE
    END AS mentions_price,
    
    CASE
        WHEN LOWER(m.message_text) LIKE '%available%'
          OR LOWER(m.message_text) LIKE '%in stock%'
          OR LOWER(m.message_text) LIKE '%ለሽያጭ%' THEN TRUE
        ELSE FALSE
    END AS mentions_availability,
    
    CASE
        WHEN LOWER(m.message_text) LIKE '%delivery%'
          OR LOWER(m.message_text) LIKE '%shipping%'
          OR LOWER(m.message_text) LIKE '%አድራሻ%' THEN TRUE
        ELSE FALSE
    END AS mentions_delivery,
    
    -- Content categorization
    CASE
        WHEN m.message_length = 0 AND m.has_image THEN 'Image Only'
        WHEN m.message_length > 0 AND m.has_image THEN 'Image with Text'
        WHEN m.message_length > 0 AND NOT m.has_image THEN 'Text Only'
        ELSE 'Empty'
    END AS content_type,
    
    -- Engagement category
    CASE
        WHEN m.view_count = 0 THEN 'No Views'
        WHEN m.view_count < 50 THEN 'Low Engagement'
        WHEN m.view_count < 200 THEN 'Medium Engagement'
        ELSE 'High Engagement'
    END AS engagement_category,
    
    -- Metadata
    m.source_file,
    m.scraped_at,
    CURRENT_TIMESTAMP AS loaded_at

FROM messages m
LEFT JOIN channels c ON m.channel_name = c.channel_name
LEFT JOIN dates d ON m.message_date = d.full_date