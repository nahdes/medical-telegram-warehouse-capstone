/*
    Fact Table: Image Detections
    YOLO object detection results joined with message facts
*/

WITH yolo_results AS (
    SELECT * FROM "medical_warehouse"."raw"."yolo_detections"
),

messages AS (
    SELECT * FROM "medical_warehouse"."public"."fct_messages"
),

channels AS (
    SELECT 
        channel_key,
        channel_name
    FROM "medical_warehouse"."public_marts"."dim_channels"
)

SELECT
    -- Surrogate key
    md5(cast(coalesce(cast(y.message_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(y.channel_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS detection_key,
    
    -- Foreign keys
    m.message_key,
    m.channel_key,
    m.date_key,
    
    -- Natural keys
    y.message_id::BIGINT AS message_id,
    c.channel_name,
    
    -- Image information
    y.image_path,
    
    -- Detection results
    y.category AS image_category,
    y.detected_objects,
    y.num_detections,
    y.max_confidence AS detection_confidence,
    
    -- Message metrics (from fact table)
    m.view_count,
    m.forward_count,
    m.message_length,
    m.message_timestamp,
    
    -- Calculated fields
    CASE 
        WHEN y.category = 'promotional' THEN TRUE
        ELSE FALSE
    END AS is_promotional,
    
    CASE 
        WHEN y.category = 'product_display' THEN TRUE
        ELSE FALSE
    END AS is_product_only,
    
    CASE
        WHEN y.num_detections > 3 THEN 'High Detail'
        WHEN y.num_detections > 1 THEN 'Medium Detail'
        WHEN y.num_detections = 1 THEN 'Low Detail'
        ELSE 'No Detection'
    END AS detail_level,
    
    CASE
        WHEN y.max_confidence >= 0.8 THEN 'High Confidence'
        WHEN y.max_confidence >= 0.5 THEN 'Medium Confidence'
        ELSE 'Low Confidence'
    END AS confidence_level,
    
    -- Metadata
    y.loaded_at AS yolo_processed_at,
    CURRENT_TIMESTAMP AS created_at

FROM yolo_results y
LEFT JOIN channels c ON y.channel_name = c.channel_name
LEFT JOIN messages m ON y.message_id::TEXT = m.message_id::TEXT 
    AND m.channel_key = c.channel_key
WHERE m.message_key IS NOT NULL  -- Only include messages that exist in fact table