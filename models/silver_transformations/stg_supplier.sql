{{
    config(
        materialized='incremental',
        unique_key='SUPPLIER_ID',
        constraints={ 'primary_key': ['SUPPLIER_ID'] }
    )
}}

WITH cleaned_suppliers AS (
    SELECT
        SUPPLIER_ID,
        {{ validate_data_types('supplier_name', 'string') }} AS supplier_name,
        {{ validate_data_types('contact_person', 'string') }} AS contact_person,
        {{ validate_email_format('email') }} AS email,
        {{ normalize_phone_number('phone') }} AS phone,
        {{ trim_whitespaces('address') }} AS address,
        {{ trim_whitespaces('payment_terms') }} AS payment_terms,
        {{ trim_whitespaces('supplier_type') }} AS supplier_type,
        {{ trim_whitespaces('category_supplied') }} AS category_supplied,
        {{ validate_data_types('contract_id', 'string') }} AS contract_id,
  contract_start_date,
 contract_end_date,
        {{ validate_data_types('renewal_option', 'boolean') }} AS renewal_option,
        {{ validate_data_types('exclusivity', 'boolean') }} AS exclusivity,
        {{ validate_data_types('on_time_delivery_rate', 'decimal(10,2)') }} AS on_time_delivery_rate,
        {{ validate_data_types('average_delay_days', 'decimal(10,2)') }} AS average_delay_days,
        {{ validate_data_types('defect_rate', 'decimal(10,2)') }} AS defect_rate,
        {{ validate_data_types('returns_percentage', 'decimal(10,2)') }} AS returns_percentage,
        {{ trim_whitespaces('quality_rating') }} AS quality_rating,
        {{ validate_data_types('response_time_hours', 'integer') }} AS response_time_hours,
        {{ validate_data_types('lead_time_days', 'integer') }} AS lead_time_days,
        {{ validate_data_types('minimum_order_quantity', 'integer') }} AS minimum_order_quantity,
        {{ trim_whitespaces('preferred_carrier') }} AS preferred_carrier,
        {{ validate_data_types('credit_rating', 'string') }} AS credit_rating,
        {{ validate_data_types('tax_id', 'string') }} AS tax_id,
        {{ validate_data_types('year_established', 'integer') }} AS year_established,
        {{ trim_whitespaces('website') }} AS website,
 last_order_date,
        {{ validate_data_types('is_active', 'boolean') }} AS is_active,
 last_modified_date
    FROM {{ source('new_data','supplier_details_silver') }}
),

supplier_transformations AS (
    SELECT
        SUPPLIER_ID,
        supplier_name,
        contact_person,
        email,
        phone,
        address,
        payment_terms,
        supplier_type,
        category_supplied,
        contract_id,
        contract_start_date,
        contract_end_date,
        renewal_option,
        exclusivity,
        on_time_delivery_rate,
        average_delay_days,
        defect_rate,
        returns_percentage,
        CASE
            WHEN quality_rating = 'excellent' THEN 'Excellent'
            WHEN quality_rating = 'good' THEN 'Good'
            WHEN quality_rating = 'satisfactory' THEN 'Satisfactory'
            WHEN quality_rating = 'fair' THEN 'Fair'
            ELSE 'Poor'
        END AS quality_rating,
        response_time_hours,
        lead_time_days,
        minimum_order_quantity,
        preferred_carrier,
        credit_rating,
        tax_id,
        year_established,
        website,
        last_order_date,
        is_active,
        last_modified_date,
        DATEDIFF(year, contract_start_date, CURRENT_DATE) AS contract_duration_years,
        (on_time_delivery_rate * 0.6) + ((100 - (average_delay_days * 10)) * 0.4) AS calculated_timeliness_score
    FROM cleaned_suppliers
    WHERE is_active = TRUE
      AND contract_end_date > CURRENT_DATE
)

SELECT * FROM supplier_transformations
