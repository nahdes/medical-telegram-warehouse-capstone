/*
    Custom Test: Assert No Future Messages
    
    This test ensures that no messages have a timestamp in the future.
    Future-dated messages would indicate data quality issues.
    
    Test passes when query returns 0 rows.
*/

SELECT
    message_id,
    channel_name,
    message_timestamp,
    CURRENT_TIMESTAMP AS current_time,
    message_timestamp - CURRENT_TIMESTAMP AS time_difference
FROM {{ ref('fct_messages') }}
WHERE message_timestamp > CURRENT_TIMESTAMP