{{
    config(
        materialized='incremental',
        unique_key='product_id',
        constraints={
            'primary_key': ['product_id']
        }
    )
}}

select
    product_id,
    UNIT_PRICE,
    COST_PRICE,
    SUPPLIER_ID,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    WEIGHT,
    DIMENSIONS,
    IS_FEATURED,
    WARRANTY_PERIOD,

    -- Standardized Pascal Case text fields
    {{ pascal_case('NAME') }} as NAME,
    {{ pascal_case('SHORT_DESCRIPTION') }} as SHORT_DESCRIPTION,
    {{ pascal_case('TECHNICAL_SPECS') }} as TECHNICAL_SPECS,
    {{ pascal_case('CATEGORY') }} as CATEGORY,
    {{ pascal_case('SUBCATEGORY') }} as SUBCATEGORY,
    {{ pascal_case('PRODUCT_LINE') }} as PRODUCT_LINE,
    {{ pascal_case('BRAND') }} as BRAND,
    {{ pascal_case('COLOR') }} as COLOR,
    {{ pascal_case('SIZE') }} as SIZE,

    -- Dates with fallback
    {{ convert_to_date('LAUNCH_DATE') }} as LAUNCH_DATE,
    {{ convert_to_date('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,

    -- Derived business logic columns
    concat(
        {{ pascal_case('NAME') }}, ' - ',
        {{ pascal_case('SHORT_DESCRIPTION') }}, ' - ',
        {{ pascal_case('TECHNICAL_SPECS') }}
    ) as FULL_DESCRIPTION,

    concat(
        {{ pascal_case('CATEGORY') }}, ' > ',
        {{ pascal_case('SUBCATEGORY') }}, ' > ',
        {{ pascal_case('PRODUCT_LINE') }}
    ) as HIERARCHICAL_CATEGORY,

    ((UNIT_PRICE - COST_PRICE) / UNIT_PRICE * 100) as PROFIT_MARGIN_PERCENTAGE,

    case
        when STOCK_QUANTITY < REORDER_LEVEL then 'Low Stock'
        else 'Sufficient Stock'
    end as STOCK_STATUS

from {{ source('new_data', 'product_details_silver') }}
