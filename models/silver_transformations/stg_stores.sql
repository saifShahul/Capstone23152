{{
    config(
        materialized='incremental',
        unique_key='store_id',
        constraints={
            'primary_key': ['store_id']
        }
    )
}}

WITH cleaned_stores AS (
    SELECT
        store_id,
        {{ standardize_pascal_case('store_name') }} AS store_name,
        {{ trim_whitespaces('street') }} AS street,
        {{ trim_whitespaces('city') }} AS city,
        {{ trim_whitespaces('state') }} AS state,
        {{ normalize_phone_number('zip_code') }} AS zip_code,
        {{ trim_whitespaces('country') }} AS country,
        {{ trim_whitespaces('region') }} AS region,
        {{ trim_whitespaces('store_type') }} AS store_type,
        {{ standardize_date_format('opening_date') }} AS opening_date,
        {{ validate_data_types('size_sq_ft', 'integer') }} AS size_sq_ft,
        {{ validate_data_types('manager_id', 'string') }} AS manager_id,
        {{ normalize_phone_number('phone_number') }} AS phone_number,
        {{ validate_email_format('email') }} AS email,
        {{ trim_whitespaces('weekdays_hours') }} AS weekdays_hours,
        {{ trim_whitespaces('weekends_hours') }} AS weekends_hours,
        {{ trim_whitespaces('holidays_hours') }} AS holidays_hours,
        {{ trim_whitespaces('service') }} AS service,
        {{ validate_data_types('employee_count', 'integer') }} AS employee_count,
        {{ validate_data_types('is_active', 'boolean') }} AS is_active,
        {{ convert_currency('monthly_rent') }} AS monthly_rent,
        {{ convert_currency('sales_target') }} AS sales_target,
        {{ convert_currency('current_sales') }} AS current_sales,
        {{ standardize_date_format('last_modified_date') }} AS last_modified_date
    FROM {{source('new_data','store_details_silver')}}
),

store_transformations AS (
    SELECT
        store_id,
        store_name,
        street,
        city,
        state,
        zip_code,
        country,
        region,
        store_type,
        opening_date,
        size_sq_ft,
        manager_id,
        phone_number,
        email,
        weekdays_hours,
        weekends_hours,
        holidays_hours,
        service,
        employee_count,
        is_active,
        monthly_rent,
        sales_target,
        current_sales,
        DATEDIFF(year, opening_date, CURRENT_DATE) AS store_age_years,
        CASE
            WHEN size_sq_ft < 5000 THEN 'Small'
            WHEN size_sq_ft BETWEEN 5000 AND 10000 THEN 'Medium'
            WHEN size_sq_ft > 10000 THEN 'Large'
        END AS store_size_category,
        (current_sales / sales_target) * 100 AS sales_target_achievement_percentage,
        (current_sales / size_sq_ft) AS revenue_per_sq_ft,
        (current_sales / employee_count) AS employee_efficiency,
        CASE
            WHEN (current_sales / sales_target) * 100 < 90 THEN 'Performance Issue'
            ELSE 'No Issue'
        END AS performance_flag,
        last_modified_date
    FROM cleaned_stores
)

SELECT * FROM store_transformations
