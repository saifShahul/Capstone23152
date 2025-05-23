{{
    config(
        materialized='incremental',
        unique_key='employee_id',
        constraints={
            'primary_key': ['employee_id']
        }
    )
}}

SELECT
    e.employee_id AS employee_id,

    {{ validate_data_types('first_name', 'string') }} AS first_name,
    {{ validate_data_types('last_name', 'string') }} AS last_name,
    CONCAT({{ validate_data_types('first_name', 'string') }}, ' ', {{ validate_data_types('last_name', 'string') }}) AS full_name,

    {{ validate_email_format('email') }} AS email,
    {{ normalize_phone_number('phone') }} AS phone,
    {{ standardize_date_format('hire_date') }} AS hire_date,
    DATEDIFF(year, {{ standardize_date_format('hire_date') }}, CURRENT_DATE) AS tenure_years,

    CASE
        WHEN {{ trim_whitespaces('role') }} ILIKE '%sales associate%' THEN 'Associate'
        WHEN {{ trim_whitespaces('role') }} ILIKE '%senior manager%' THEN 'Senior Manager'
        WHEN {{ trim_whitespaces('role') }} ILIKE '%store manager%' THEN 'Manager'
        ELSE {{ trim_whitespaces('role') }}
    END AS standardized_role,

    {{ trim_whitespaces('department') }} AS department,
    {{ trim_whitespaces('work_location') }} AS work_location,

    {{ convert_currency('salary') }} AS salary,
    {{ validate_data_types('manager_id', 'string') }} AS manager_id,
    {{ trim_whitespaces('employment_status') }} AS employment_status,
    {{ trim_whitespaces('education') }} AS education,

    {{ trim_whitespaces('street') }} AS street,
    {{ trim_whitespaces('city') }} AS city,
    {{ trim_whitespaces('state') }} AS state,
    {{ normalize_phone_number('zip_code') }} AS zip_code,

    {{ standardize_date_format('date_of_birth') }} AS date_of_birth,

    {{ convert_currency('sales_target') }} AS sales_target,
    {{ convert_currency('current_sales') }} AS current_sales,

    ({{ convert_currency('current_sales') }} / NULLIF({{ convert_currency('sales_target') }}, 0)) * 100 AS target_achievement_percentage,


    COUNT(o.order_id) AS orders_processed,


    SUM({{ convert_currency('total_amount') }}) AS total_sales_amount,

    {{ validate_data_types('performance_rating', 'decimal(10,2)') }} AS performance_rating,
    {{ trim_whitespaces('certification') }} AS certification,
    {{ standardize_date_format('e.last_modified_date') }} AS last_modified_date

FROM {{ source('new_data','employee_details_silver') }} AS e

LEFT JOIN {{ source('new_data','order_details_silver') }} AS o
    ON e.employee_id = o.employee_id

GROUP BY
    e.employee_id,
    e.first_name,
    e.last_name,
    e.email,
    e.phone,
    e.hire_date,
    e.role,
    e.department,
    e.work_location,
    e.salary,
    e.manager_id,
    e.employment_status,
    e.education,
    e.street,
    e.city,
    e.state,
    e.zip_code,
    e.date_of_birth,
    e.sales_target,
    e.current_sales,
    e.performance_rating,
    e.certification,
    e.last_modified_date



