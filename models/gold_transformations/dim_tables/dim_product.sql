{{
    config(
        materialized = 'incremental',
        unique_key = 'product_id',
        constraints = {
            'primary_key': ['product_id']
        }
    )
}}

select
    md5(cast(product_id as string)) as product_key,  -- surrogate key
    product_id,
    name as product_name,
    category,
    subcategory,
    brand,
    color,
    size,
    unit_price,
    cost_price,
    supplier_id as supplier_information

from DBT_SAIFSHAHUL_NEW_DATA.STG_PRODUCTS
