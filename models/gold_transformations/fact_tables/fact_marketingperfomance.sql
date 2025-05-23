WITH campaign_base AS (
    SELECT
        MD5(CAST(campaign_id AS STRING)) AS campaign_key,
        campaign_id,
        target_audience,
        budget,
        start_date,
        end_date
    FROM {{ source('DBT_SAIFSHAHUL_NEW_DATA', 'STG_MARKETING') }}
),

sales_base AS (
    SELECT
        MD5(CONCAT(od.order_id, od.customer_id, dp.product_id, od.store_id, od.employee_id, od.campaign_id)) AS sales_key,
        od.order_id,
        od.customer_id,
        od.product_id,
        od.store_id,
        od.employee_id,
        od.campaign_id,
        od.order_date,
        TO_DATE(od.order_date) AS date_key,
        od.total__sales_amount,
        od.quantity,
        dp.cost_price
    FROM DBT_SAIFSHAHUL_NEW_DATA.STG_ORDERS od
    JOIN DBT_SAIFSHAHUL_NEW_DATA.dim_product dp ON od.product_id = dp.product_id
    WHERE od.campaign_id IS NOT NULL
),

sales_with_campaign AS (
    SELECT
        cb.campaign_key,
        sb.*
    FROM campaign_base cb
    JOIN sales_base sb ON cb.campaign_id = sb.campaign_id
    WHERE TO_DATE(sb.order_date) BETWEEN TO_DATE(cb.start_date) AND TO_DATE(cb.end_date)
),

first_purchases AS (
    SELECT
        customer_id,
        MIN(TO_DATE(order_date)) AS first_order_date
    FROM DBT_SAIFSHAHUL_NEW_DATA.STG_ORDERS
    GROUP BY customer_id
),

new_customers AS (
    SELECT
        swc.campaign_key,
        COUNT(DISTINCT swc.customer_id) AS new_customers_acquired
    FROM sales_with_campaign swc
    JOIN first_purchases fp ON swc.customer_id = fp.customer_id
    JOIN campaign_base cb ON swc.campaign_key = cb.campaign_key
    WHERE TO_DATE(fp.first_order_date) BETWEEN TO_DATE(cb.start_date) AND TO_DATE(cb.end_date)
    GROUP BY swc.campaign_key
),

repeat_customers AS (
    SELECT
        campaign_key,
        COUNT(DISTINCT customer_id) AS repeat_customers
    FROM (
        SELECT 
            customer_id,
            campaign_key,
            COUNT(order_id) AS total_orders
        FROM sales_with_campaign
        GROUP BY customer_id, campaign_key
        HAVING COUNT(order_id) > 1
    )
    GROUP BY campaign_key
),

first_time_customers AS (
    SELECT
        campaign_key,
        COUNT(DISTINCT customer_id) AS first_time_customers
    FROM sales_with_campaign
    GROUP BY campaign_key
),

final_metrics AS (
    SELECT
        cb.campaign_key,
        MAX(cb.start_date) AS campaign_start_date,
        MAX(cb.end_date) AS campaign_end_date,
        SUM(swc.total__sales_amount) AS total_sales_influenced,
        MAX(cb.budget) AS total_campaign_cost,
        COALESCE(nc.new_customers_acquired, 0) AS new_customers_acquired,
        COALESCE(rc.repeat_customers, 0) AS repeat_customers,
        COALESCE(fc.first_time_customers, 1) AS first_time_customers,
        ROUND((COALESCE(rc.repeat_customers, 0) * 100.0) / COALESCE(fc.first_time_customers, 1), 2) AS repeat_purchase_rate,
        ROUND(((SUM(swc.total__sales_amount) - MAX(cb.budget)) / NULLIF(MAX(cb.budget), 0)) * 100, 2) AS roi_percentage
    FROM campaign_base cb
    LEFT JOIN sales_with_campaign swc ON cb.campaign_key = swc.campaign_key
    LEFT JOIN new_customers nc ON cb.campaign_key = nc.campaign_key
    LEFT JOIN repeat_customers rc ON cb.campaign_key = rc.campaign_key
    LEFT JOIN first_time_customers fc ON cb.campaign_key = fc.campaign_key
    GROUP BY cb.campaign_key, cb.start_date, cb.end_date, cb.budget
)

-- FINAL SELECT to prevent dbt inlining issues
SELECT
    campaign_key,
    campaign_start_date,
    campaign_end_date,
    total_sales_influenced,
    total_campaign_cost,
    new_customers_acquired,
    repeat_customers,
    first_time_customers,
    repeat_purchase_rate,
    roi_percentage
FROM final_metrics
