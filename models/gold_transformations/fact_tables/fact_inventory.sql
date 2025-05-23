{{
  config(
    materialized = 'incremental',
    unique_key = 'inventory_key',
    constraints = {
      'primary_key': ['inventory_key']
    }
  )
}}

SELECT 
    md5(dp.product_key || st.store_key || su.supplier_key) AS inventory_key,
    dp.product_key AS product_key,
    st.store_key AS store_key,
    su.supplier_key AS supplier_key,
    p.stock_quantity AS beginning_inventory,
    SUM(o.quantity) AS PurchasedQuantity,
    SUM(o.quantity) AS SoldQuantity,
    p.stock_quantity - SUM(o.quantity) AS ending_inventory,
    (p.stock_quantity * p.unit_price) AS InventoryValue,
    (SUM(o.quantity) / NULLIF((p.stock_quantity + (p.stock_quantity - SUM(o.quantity))) / 2, 0)) AS StockTurnoverRatio
FROM
    DBT_SAIFSHAHUL_NEW_DATA.STG_ORDERS o
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_product dp ON dp.product_id = o.product_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_stores st ON st.store_id = o.store_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.stg_products p ON p.product_id = o.product_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_supplier su ON su.supplier_id = p.supplier_id
GROUP BY
    dp.product_key, st.store_key, su.supplier_key, p.stock_quantity, p.unit_price
/*
SELECT 
    md5(dp.product_key) AS inventory_key,
    dp.product_key AS product_key,
    st.store_key AS store_key,
    su.supplier_key AS supplier_key,
    p.stock_quantity AS beginning_inventory,
    SUM(o.quantity) AS PurchasedQuantity,
    SUM(o.quantity) AS SoldQuantity,
    p.stock_quantity - SUM(o.quantity) AS ending_inventory,
    (p.stock_quantity * p.unit_price) AS InventoryValue,
    (SUM(o.quantity) / NULLIF((p.stock_quantity + (p.stock_quantity - SUM(o.quantity))) / 2, 0)) AS StockTurnoverRatio
FROM
    DBT_SAIFSHAHUL_NEW_DATA.STG_ORDERS o
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_product dp ON dp.product_id = o.product_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_stores st ON st.store_id = o.store_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.stg_products p ON p.product_id = o.product_id
JOIN
    DBT_SAIFSHAHUL_NEW_DATA.dim_supplier su ON su.supplier_id = p.supplier_id
GROUP BY
    dp.product_key, st.store_key, su.supplier_key, p.stock_quantity, p.unit_price
 
*/