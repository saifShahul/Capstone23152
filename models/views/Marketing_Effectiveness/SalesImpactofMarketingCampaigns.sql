{{ config(materialized = 'view') }}

SELECT
    dc.campaign_id,
    dc.start_date,
    dc.end_date,
    COUNT(DISTINCT fs.OrderID) AS total_orders,
    SUM(fs.TotalSalesAmount) AS total_sales_generated,
    AVG(fs.TotalSalesAmount) AS avg_sales_per_order
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_marketingcampaign') }} AS dc
    ON fs.CampaignKey = dc.campaign_id
GROUP BY dc.campaign_id, dc.start_date, dc.end_date
ORDER BY total_sales_generated DESC