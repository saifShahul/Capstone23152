{{
  config(
    materialized = 'incremental',
    unique_key ='DateKey',
    constraints = {
      'primary_key': ['DateKey']
    }
  )
}}

SELECT
    MD5(TO_CHAR(DATEADD(day, SEQ4(), '2024-01-01'), 'YYYY-MM-DD')) AS DateKey,
    DATEADD(day, SEQ4(), '2024-01-01') AS FullDate,
    YEAR(DATEADD(day, SEQ4(), '2024-01-01')) AS Year,
    QUARTER(DATEADD(day, SEQ4(), '2024-01-01')) AS Quarter,
    MONTH(DATEADD(day, SEQ4(), '2024-01-01')) AS Month,
    WEEK(DATEADD(day, SEQ4(), '2024-01-01')) AS Week,
    DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01')) AS DayOfWeek,
    CASE 
        WHEN DATEADD(day, SEQ4(), '2024-01-01') IN ('2024-01-01', '2024-07-04', '2024-12-25') THEN 1 -- Example US Holidays
        ELSE 0
    END AS HolidayFlag,
    CASE 
        WHEN MONTH(DATEADD(day, SEQ4(), '2024-01-01')) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(DATEADD(day, SEQ4(), '2024-01-01')) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(DATEADD(day, SEQ4(), '2024-01-01')) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(DATEADD(day, SEQ4(), '2024-01-01')) IN (9, 10, 11) THEN 'Fall'
    END AS Season
FROM TABLE(GENERATOR(ROWCOUNT => 366)) -- Generates dates for the year 2024 (leap year)
