{{
    config(
        materialized = 'incremental',
        unique_key = 'store_id',
        constraints = {
            'primary_key': ['store_id']
        }
    )
}}

select
    md5(cast(store_id as string)) as store_key,  -- surrogate key
    store_id,
    store_name,
    concat(street, ', ', city, ', ', state, ' ', zip_code, ', ', country) as address,
    region,
    store_type,
    opening_date,
    store_size_category as size_category

from {{ source('DBT_SAIFSHAHUL_NEW_DATA','STG_STORES') }}
