{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    constraints = {
      'primary_key': ['order_id']
    }
  )
}}

SELECT
    -- Validate Data using Macros
    {{ validate_data_types('order_id', 'string') }} AS order_id,
    {{ validate_data_types('customer_id', 'integer') }} AS customer_id,
    TO_DATE(order_date) as order_date,
    to_date(shipping_date) as shipping_date,
    to_date(delivery_date) as delivery_date,
    to_date(estimated_delivery_date) as estimated_delivery_date,
    unit_price,
    quantity,
    product_id,
    
    -- Additional columns
    {{ validate_data_types('store_id', 'string') }} AS store_id,
    {{ validate_data_types('employee_id', 'string') }} AS employee_id,
    {{ validate_data_types('order_source', 'string') }} AS order_source,
    {{ validate_data_types('campaign_id', 'string') }} AS campaign_id,
    to_date(created_at) AS created_at,
    
    -- Other columns (apply macros where needed)
    {{ convert_currency('total_amount') }} AS total_amount,
    {{ convert_currency('discount_amount') }} AS discount_amount,
    {{ convert_currency('shipping_cost') }} AS shipping_cost,
    {{ convert_currency('tax_amount') }} AS tax_amount,
    
    -- Time of Day Calculation using EXTRACT
    CASE 
        WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(order_date)) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(order_date)) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(order_date)) BETWEEN 17 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS order_time_of_day,

    -- Week, Month, Quarter, Year Calculation from order_date
    EXTRACT(WEEK FROM TO_DATE(order_date)) AS order_week,
    EXTRACT(MONTH FROM to_date(order_date)) AS order_month,
    EXTRACT(QUARTER FROM to_date(order_date) ) AS order_quarter,
    EXTRACT(YEAR FROM to_date(order_date)) AS order_year,

    -- Aggregates
    COUNT(product_id) OVER (PARTITION BY order_id) AS total_items,
    SUM(quantity) OVER (PARTITION BY order_id) AS total_quantity,
    SUM(quantity * unit_price) OVER (PARTITION BY order_id) AS total__sales_amount,
    SUM(quantity * cost_price) OVER (PARTITION BY order_id) AS total_cost,
    SUM(discount_amount) OVER (PARTITION BY order_id) AS total_discount,

    -- Profitability Calculation
    (total_amount - (total_cost + total_discount + shipping_cost + tax_amount)) AS profit_amount,
    (total_amount - (total_cost + total_discount + shipping_cost + tax_amount)) / total_amount AS profit_margin_percentage,

    -- Shipping Efficiency Metrics
    DATEDIFF(day, order_date, shipping_date) AS processing_days,
    DATEDIFF(day, shipping_date, delivery_date) AS shipping_days,
    CASE 
        WHEN delivery_date IS NOT NULL AND delivery_date <= estimated_delivery_date THEN 'On Time'
        WHEN delivery_date IS NOT NULL AND delivery_date > estimated_delivery_date THEN 'Delayed'
        WHEN delivery_date IS NULL AND CURRENT_DATE > estimated_delivery_date THEN 'Potentially Delayed'
        ELSE 'In Transit'
    END AS delivery_status

FROM {{ source('new_data','order_details_silver') }}

