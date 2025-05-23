{{ config(
    materialized = 'view'
) }}

WITH inventory_data AS (
    SELECT
        product_key AS ProductKey,
        store_key AS StoreKey,
        supplier_key AS SupplierKey,
        beginning_inventory AS BeginningInventory,
        SUM(SoldQuantity) AS TotalQuantitySold,
        ending_inventory AS EndingInventory,
        InventoryValue,
        StockTurnoverRatio
    FROM 
        {{ ref('fact_inventory') }}
    GROUP BY 
        product_key, 
        store_key, 
        supplier_key, 
        beginning_inventory, 
        ending_inventory, 
        InventoryValue, 
        StockTurnoverRatio
),

slow_fast_moving_products AS (
    SELECT
        ProductKey,
        CASE 
            WHEN TotalQuantitySold > (SELECT AVG(TotalQuantitySold) FROM inventory_data) THEN 'Fast-moving'
            ELSE 'Slow-moving'
        END AS ProductMovement
    FROM 
        inventory_data
)

SELECT
    id.ProductKey,
    id.StoreKey,
    id.SupplierKey,
    id.BeginningInventory,
    id.TotalQuantitySold AS PurchasedQuantity,
    id.TotalQuantitySold AS SoldQuantity,
    id.EndingInventory,
    id.InventoryValue,
    id.StockTurnoverRatio,
    sfmp.ProductMovement
FROM 
    inventory_data id
JOIN 
    slow_fast_moving_products sfmp ON id.ProductKey = sfmp.ProductKey
ORDER BY 
    id.StockTurnoverRatio DESC
