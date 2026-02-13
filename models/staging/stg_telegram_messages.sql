/*
    Staging model for Telegram messages
    Cleans and standardizes raw telegram data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        message_id,
        channel_name,
        
        -- Dates and timestamps
        message_date::TIMESTAMP AS message_timestamp,
        message_date::DATE AS message_date,
        scraped_at::TIMESTAMP AS scraped_at,
        
        -- Message content
        TRIM(message_text) AS message_text,
        LENGTH(TRIM(message_text)) AS message_length,
        
        -- Media flags
        COALESCE(has_media, FALSE) AS has_media,
        CASE 
            WHEN has_media AND image_path IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_image,
        image_path,
        
        -- Engagement metrics
        COALESCE(views, 0) AS view_count,
        COALESCE(forwards, 0) AS forward_count,
        
        -- Reply information
        COALESCE(is_reply, FALSE) AS is_reply,
        reply_to_msg_id,
        
        -- Metadata
        source_file,
        
        -- Calculated fields
        CASE
            WHEN LENGTH(TRIM(message_text)) = 0 THEN TRUE
            ELSE FALSE
        END AS is_empty_message,
        
        -- Extract time components
        EXTRACT(HOUR FROM message_date) AS message_hour,
        EXTRACT(DOW FROM message_date) AS day_of_week,
        
        -- Derive channel type based on channel name
        CASE
            WHEN LOWER(channel_name) LIKE '%pharm%' THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) LIKE '%cosmet%' THEN 'Cosmetics'
            WHEN LOWER(channel_name) LIKE '%med%' THEN 'Medical'
            ELSE 'Other'
        END AS channel_type

    FROM source
    
    -- Data quality filters
    WHERE 
        message_id IS NOT NULL
        AND channel_name IS NOT NULL
        AND message_date IS NOT NULL
        AND message_date <= CURRENT_TIMESTAMP  -- No future dates
)

SELECT * FROM cleaned