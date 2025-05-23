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
    md5(cast(employee_id as string)) AS employee_key,  -- Surrogate Key
    employee_id,
    full_name,
    standardized_role as role,
    work_location,
    tenure_years,
    email AS email_id,
    phone AS phone_number,
    target_achievement_percentage,
    orders_processed,
    total_sales_amount,
    performance_rating
from {{source('DBT_SAIFSHAHUL_NEW_DATA', 'STG_EMPLOYEES') }}

