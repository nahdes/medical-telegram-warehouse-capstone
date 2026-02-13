/*
    Custom Test: Assert No Orphan Facts
    
    This test ensures all facts have valid dimension references.
    Orphan facts would indicate referential integrity issues.
    
    Test passes when query returns 0 rows.
*/

-- Check for facts with missing channel dimension
SELECT
    'Missing Channel' AS issue_type,
    f.message_id,
    f.channel_key,
    COUNT(*) AS issue_count
FROM "medical_warehouse"."public"."fct_messages" f
LEFT JOIN "medical_warehouse"."public_marts"."dim_channels" c ON f.channel_key = c.channel_key
WHERE c.channel_key IS NULL
GROUP BY f.message_id, f.channel_key

UNION ALL

-- Check for facts with missing date dimension
SELECT
    'Missing Date' AS issue_type,
    f.message_id,
    f.date_key::TEXT AS channel_key,
    COUNT(*) AS issue_count
FROM "medical_warehouse"."public"."fct_messages" f
LEFT JOIN "medical_warehouse"."public_marts"."dim_dates" d ON f.date_key = d.date_key
WHERE d.date_key IS NULL
GROUP BY f.message_id, f.date_key