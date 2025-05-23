{{ config(materialized = 'view') }}

SELECT
    dc.campaign_id,
    COUNT(DISTINCT fs.CustomerKey) AS engaged_customers,
    COUNT(fs.orderID) AS total_campaign_orders,
    SUM(fs.TotalSalesAmount) AS total_campaign_sales,
    ROUND(SUM(fs.TotalSalesAmount) / NULLIF(COUNT(DISTINCT fs.CustomerKey), 0), 2) AS avg_sales_per_customer
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_marketingcampaign') }} AS dc
    ON fs.CampaignKey = dc.campaign_id
GROUP BY dc.campaign_id
ORDER BY engaged_customers DESC