{{
    config(
        materialized = 'incremental',
        unique_key = 'SUPPLIER_ID',
        constraints = {
            'primary_key': ['SUPPLIER_ID']
        }
    )
}}

select
    md5(cast(SUPPLIER_ID as string)) as supplier_key,  -- surrogate key
    SUPPLIER_ID,
    supplier_name,
    contact_person,
    email,
    phone,
    address,
    payment_terms,
    supplier_type,
    on_time_delivery_rate as Avg_Delivery_Timeliness_Score,
    quality_rating,
    is_active as Active_Contract_Details

from {{ source('DBT_SAIFSHAHUL_NEW_DATA','STG_SUPPLIER')}}

