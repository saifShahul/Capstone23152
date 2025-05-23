{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    constraints = {
      'primary_key': ['customer_id']
    }
  )
}}

select
    md5(cast(customer_id as string)) as customer_key,
    customer_id,
    full_name,
    email,
    phone,
    standardized_address as address_details,
    birth_date,
    age,
    occupation,
    income_bracket,
    loyalty_tier,
    segment,
    registration_date
from {{source('DBT_SAIFSHAHUL_NEW_DATA', 'STG_CUSTOMERS') }}
