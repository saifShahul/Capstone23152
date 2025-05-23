{{ config(materialized = 'view') }}

SELECT
    ds.supplier_id,
    ds.supplier_name,
    fs.order_date,
    ds.quality_rating,
    COUNT(*) AS rating_count
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_product') }} AS dp
    ON fs.ProductKey = dp.product_id
JOIN {{ ref('dim_supplier') }} AS ds
    ON dp.supplier_information = ds.supplier_id
WHERE ds.quality_rating IS NOT NULL
GROUP BY ds.supplier_id, ds.supplier_name, fs.order_date, ds.quality_rating
ORDER BY ds.supplier_id, fs.order_date
