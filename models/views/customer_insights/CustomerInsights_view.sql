{{ config(
    materialized = 'view'
) }}

WITH customer_lifetime_value AS (
    SELECT
        fs.CustomerKey,
        SUM(fs.TotalSalesAmount) AS LifetimeValue
    FROM 
        {{ ref('facts_sales') }} AS fs
    GROUP BY 
        fs.CustomerKey
),

customer_purchases AS (
    SELECT
        fs.CustomerKey,
        COUNT(*) AS PurchaseCount
    FROM 
        {{ ref('facts_sales') }} AS fs
    GROUP BY 
        fs.CustomerKey
),

repeat_purchase_rate AS (
    SELECT
        fs.CustomerKey,
        (COUNT(DISTINCT CASE WHEN cp.PurchaseCount > 1 THEN fs.CustomerKey END) * 100.0) / 
        COUNT(DISTINCT fs.CustomerKey) AS RepeatPurchaseRate
    FROM 
        {{ ref('facts_sales') }} AS fs
    JOIN 
        customer_purchases cp ON fs.CustomerKey = cp.CustomerKey
    GROUP BY 
        fs.CustomerKey
),

customer_segmentation AS (
    SELECT
        fs.CustomerKey,
        dc.segment AS CustomerSegment,
        COUNT(DISTINCT fs.OrderID) AS NumberOfOrders,
        SUM(fs.TotalSalesAmount) AS TotalSales,
        AVG(fs.TotalSalesAmount) AS AverageOrderValue
    FROM 
        {{ ref('facts_sales') }} AS fs
    JOIN 
        {{ ref('dim_customer') }} dc ON fs.CustomerKey = dc.customer_id
    GROUP BY 
        fs.CustomerKey, 
        dc.segment
)

SELECT
    dc.customer_id AS CustomerID,
    dc.full_name AS CustomerName,
    clv.LifetimeValue,
    rpr.RepeatPurchaseRate,
    cs.CustomerSegment,
    cs.NumberOfOrders,
    cs.TotalSales,
    cs.AverageOrderValue
FROM 
    {{ ref('dim_customer') }} dc
JOIN 
    customer_lifetime_value clv ON dc.customer_id = clv.CustomerKey
JOIN 
    repeat_purchase_rate rpr ON dc.customer_id = rpr.CustomerKey
JOIN 
    customer_segmentation cs ON dc.customer_id = cs.CustomerKey
ORDER BY 
    clv.LifetimeValue DESC
