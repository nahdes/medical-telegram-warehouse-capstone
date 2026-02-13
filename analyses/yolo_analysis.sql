/*
    YOLO Detection Analysis Queries
    Answer key business questions about image content
*/

-- 1. Do promotional posts get more views than product_display posts?
SELECT 
    image_category,
    COUNT(*) as post_count,
    ROUND(AVG(view_count)::NUMERIC, 0) as avg_views,
    ROUND(AVG(forward_count)::NUMERIC, 1) as avg_forwards,
    MAX(view_count) as max_views
FROM {{ ref('fct_image_detections') }}
GROUP BY image_category
ORDER BY avg_views DESC;

-- 2. Which channels use more visual content?
SELECT 
    c.channel_name,
    c.channel_type,
    COUNT(d.detection_key) as images_with_detections,
    c.total_posts,
    ROUND(COUNT(d.detection_key)::NUMERIC * 100 / c.total_posts, 1) as detection_pct,
    ROUND(AVG(d.num_detections)::NUMERIC, 1) as avg_objects_per_image,
    ROUND(AVG(d.detection_confidence)::NUMERIC, 3) as avg_confidence
FROM {{ ref('dim_channels') }} c
LEFT JOIN {{ ref('fct_image_detections') }} d ON c.channel_key = d.channel_key
GROUP BY c.channel_name, c.channel_type, c.total_posts
ORDER BY images_with_detections DESC;

-- 3. Image category performance by channel
SELECT 
    c.channel_name,
    d.image_category,
    COUNT(*) as count,
    ROUND(AVG(d.view_count)::NUMERIC, 0) as avg_views
FROM {{ ref('fct_image_detections') }} d
JOIN {{ ref('dim_channels') }} c ON d.channel_key = c.channel_key
GROUP BY c.channel_name, d.image_category
ORDER BY c.channel_name, avg_views DESC;

-- 4. Detection confidence vs engagement
SELECT 
    confidence_level,
    COUNT(*) as image_count,
    ROUND(AVG(view_count)::NUMERIC, 0) as avg_views,
    ROUND(AVG(forward_count)::NUMERIC, 1) as avg_forwards
FROM {{ ref('fct_image_detections') }}
GROUP BY confidence_level
ORDER BY 
    CASE confidence_level
        WHEN 'High Confidence' THEN 1
        WHEN 'Medium Confidence' THEN 2
        WHEN 'Low Confidence' THEN 3
    END;

-- 5. Most common detected objects
SELECT 
    TRIM(UNNEST(STRING_TO_ARRAY(detected_objects, ','))) as object_class,
    COUNT(*) as frequency
FROM {{ ref('fct_image_detections') }}
WHERE detected_objects != ''
GROUP BY object_class
ORDER BY frequency DESC
LIMIT 20;

-- 6. Promotional vs Product Display - Statistical Comparison
SELECT 
    'Promotional (Person + Product)' as category,
    COUNT(*) as n,
    ROUND(AVG(view_count)::NUMERIC, 0) as avg_views,
    ROUND(STDDEV(view_count)::NUMERIC, 0) as stddev_views,
    MIN(view_count) as min_views,
    MAX(view_count) as max_views
FROM {{ ref('fct_image_detections') }}
WHERE image_category = 'promotional'

UNION ALL

SELECT 
    'Product Display (Product Only)' as category,
    COUNT(*) as n,
    ROUND(AVG(view_count)::NUMERIC, 0) as avg_views,
    ROUND(STDDEV(view_count)::NUMERIC, 0) as stddev_views,
    MIN(view_count) as min_views,
    MAX(view_count) as max_views
FROM {{ ref('fct_image_detections') }}
WHERE image_category = 'product_display';

-- 7. Time trends for different image categories
SELECT 
    dt.year_month,
    d.image_category,
    COUNT(*) as post_count,
    ROUND(AVG(d.view_count)::NUMERIC, 0) as avg_views
FROM {{ ref('fct_image_detections') }} d
JOIN {{ ref('dim_dates') }} dt ON d.date_key = dt.date_key
GROUP BY dt.year_month, d.image_category
ORDER BY dt.year_month, avg_views DESC;