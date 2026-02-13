/*
    Custom Test: Assert Positive Views
    
    This test ensures that view counts and forward counts are non-negative.
    Negative engagement metrics would indicate data corruption.
    
    Test passes when query returns 0 rows.
*/

SELECT
    f.message_id,
    c.channel_name,
    f.view_count,
    f.forward_count,
    f.message_timestamp
FROM {{ ref('fct_messages') }} f
LEFT JOIN {{ ref('dim_channels') }} c ON f.channel_key = c.channel_key
WHERE 
    f.view_count < 0 
    OR f.forward_count < 0
    OR f.forward_count > f.view_count  -- Can't have more forwards than views