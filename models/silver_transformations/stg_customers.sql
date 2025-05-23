{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        constraints={ 'primary_key': ['customer_id'] }
    )
}}

with customer_data as (
    select
        -- Applying macros to clean and transform the data
        {{ validate_data_types('customer_id', 'string') }} as customer_id,
        {{ trim_whitespaces('FIRST_NAME') }} as FIRST_NAME,
        {{ trim_whitespaces('LAST_NAME') }} as LAST_NAME,
        {{ validate_email_format('EMAIL') }} as EMAIL,
        {{ normalize_phone_number('PHONE') }} as PHONE,
        {{ convert_to_date('BIRTH_DATE') }} as BIRTH_DATE,

        -- Age calculation based on birth date
        datediff(year, {{ convert_to_date('BIRTH_DATE') }}, current_date) as AGE,

        -- Customer segmentation based on age
        case
            when datediff(year, {{ convert_to_date('BIRTH_DATE') }}, current_date) between 18 and 35 then 'Young'
            when datediff(year, {{ convert_to_date('BIRTH_DATE') }}, current_date) between 36 and 55 then 'Middle-aged'
            else 'Senior'
        end as SEGMENT,

        -- Full name as concatenation of first and last name
        concat({{ trim_whitespaces('FIRST_NAME') }}, ' ', {{ trim_whitespaces('LAST_NAME') }}) as FULL_NAME,
        -- Standardized address from multiple columns
        concat(
            {{ trim_whitespaces('STREET') }}, ', ',
            {{ trim_whitespaces('CITY') }}, ', ',
            {{ trim_whitespaces('STATE') }}, ' ',
            {{ trim_whitespaces('ZIP_CODE') }}, ', ',
            {{ trim_whitespaces('COUNTRY') }}
        ) as STANDARDIZED_ADDRESS,

        -- Converting total spend column to a standardized format
        {{ convert_currency('TOTAL_SPEND') }} as TOTAL_SPEND,

        -- Retaining raw columns or applying additional transformations if needed
        {{ convert_to_date('REGISTRATION_DATE') }} as REGISTRATION_DATE,
        {{ convert_to_date('LAST_PURCHASE_DATE') }} as LAST_PURCHASE_DATE,
        {{ convert_to_date('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,
        {{ trim_whitespaces('PREFERRED_COMMUNICATION') }} as PREFERRED_COMMUNICATION,
        {{ trim_whitespaces('OCCUPATION') }} as OCCUPATION,
        {{ trim_whitespaces('INCOME_BRACKET') }} as INCOME_BRACKET,
        {{ trim_whitespaces('LOYALTY_TIER') }} as LOYALTY_TIER,
        {{ trim_whitespaces('TOTAL_PURCHASES') }} as TOTAL_PURCHASES,
        {{ trim_whitespaces('PREFERRED_PAYMENT_METHOD') }} as PREFERRED_PAYMENT_METHOD,
        {{ trim_whitespaces('MARKETING_OPT_IN') }} as MARKETING_OPT_IN
        
    from {{ source('new_data','customer_details_silver') }}
)

select * from customer_data
