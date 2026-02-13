/*
    Dimension: Dates
    Date dimension table for time-based analysis
    Generates dates from the earliest message to today + 1 year
*/

WITH date_spine AS (
    -- Get min and max dates from messages
    SELECT
        MIN(message_date)::DATE AS min_date,
        CURRENT_DATE + INTERVAL '1 year' AS max_date
    FROM "medical_warehouse"."public_staging"."stg_telegram_messages"
),

date_range AS (
    SELECT
        generate_series(
            (SELECT min_date FROM date_spine),
            (SELECT max_date FROM date_spine),
            '1 day'::INTERVAL
        )::DATE AS full_date
)

SELECT
    -- Surrogate key (YYYYMMDD format)
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    
    -- Full date
    full_date,
    
    -- Day attributes
    EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week,  -- 0=Sunday, 6=Saturday
    EXTRACT(DOY FROM full_date)::INTEGER AS day_of_year,
    TO_CHAR(full_date, 'Day') AS day_name,
    TO_CHAR(full_date, 'Dy') AS day_name_short,
    
    -- Week attributes
    EXTRACT(WEEK FROM full_date)::INTEGER AS week_of_year,
    TO_CHAR(full_date, 'IYYY-IW') AS year_week,
    DATE_TRUNC('week', full_date)::DATE AS week_start_date,
    
    -- Month attributes
    EXTRACT(MONTH FROM full_date)::INTEGER AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    TO_CHAR(full_date, 'Mon') AS month_name_short,
    TO_CHAR(full_date, 'YYYY-MM') AS year_month,
    DATE_TRUNC('month', full_date)::DATE AS month_start_date,
    
    -- Quarter attributes
    EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
    'Q' || EXTRACT(QUARTER FROM full_date)::TEXT AS quarter_name,
    TO_CHAR(full_date, 'YYYY-"Q"Q') AS year_quarter,
    DATE_TRUNC('quarter', full_date)::DATE AS quarter_start_date,
    
    -- Year attributes
    EXTRACT(YEAR FROM full_date)::INTEGER AS year,
    
    -- Flags
    CASE 
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    
    CASE 
        WHEN EXTRACT(DOW FROM full_date) NOT IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekday,
    
    -- Ethiopian calendar helper (approximate)
    CASE
        WHEN EXTRACT(MONTH FROM full_date) = 9 AND EXTRACT(DAY FROM full_date) >= 11 THEN 'New Year'
        WHEN EXTRACT(MONTH FROM full_date) = 9 AND EXTRACT(DAY FROM full_date) = 27 THEN 'Meskel'
        WHEN EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 7 THEN 'Genna'
        WHEN EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 19 THEN 'Timkat'
        ELSE NULL
    END AS ethiopian_holiday,
    
    -- Relative date attributes
    CASE
        WHEN full_date = CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_today,
    
    CASE
        WHEN full_date = CURRENT_DATE - 1 THEN TRUE
        ELSE FALSE
    END AS is_yesterday,
    
    CASE
        WHEN full_date >= DATE_TRUNC('week', CURRENT_DATE)
        AND full_date < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'
        THEN TRUE
        ELSE FALSE
    END AS is_current_week,
    
    CASE
        WHEN full_date >= DATE_TRUNC('month', CURRENT_DATE)
        AND full_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
        THEN TRUE
        ELSE FALSE
    END AS is_current_month,
    
    CASE
        WHEN full_date >= DATE_TRUNC('year', CURRENT_DATE)
        AND full_date < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'
        THEN TRUE
        ELSE FALSE
    END AS is_current_year

FROM date_range