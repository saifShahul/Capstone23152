{{ config(
    materialized = 'view'
) }}

SELECT
    dp.category AS ProductCategory,
    SUM(fs.TotalSalesAmount) AS TotalSales
FROM 
    {{ ref('facts_sales') }} AS fs
JOIN 
    {{ ref('dim_product') }} AS dp ON fs.ProductKey = dp.product_id
GROUP BY 
    dp.category
ORDER BY 
    TotalSales DESC
