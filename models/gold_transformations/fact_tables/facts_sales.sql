{{
  config(
    materialized = 'incremental',
    unique_key = 'SalesKey',
    constraints = {
      'primary_key': ['SalesKey']
    }
  )
}}

SELECT
    MD5(CONCAT(od.order_id, od.customer_id, dp.product_id, od.store_id, od.employee_id, od.campaign_id)) AS SalesKey,
    od.order_id AS OrderID,
    od.customer_id AS CustomerKey,
    dp.product_id AS ProductKey,
    od.store_id AS StoreKey,
    od.order_date as order_date,
    MD5(TO_CHAR(od.order_date, 'YYYY-MM-DD')) AS DateKey,
    od.employee_id AS EmployeeKey,
    od.campaign_id AS CampaignKey,
    SUM(od.quantity) AS QuantitySold,
    AVG(od.unit_price) AS UnitPrice,
    SUM(od.total__sales_amount) AS TotalSalesAmount,
    SUM(od.quantity * dp.cost_price) AS CostAmount,
    SUM(od.quantity * od.unit_price - od.quantity * dp.cost_price - od.discount_amount - od.shipping_cost) AS ProfitAmount,
    SUM(od.discount_amount) AS DiscountAmount,
    SUM(od.shipping_cost) AS ShippingCost,
    SPLIT_PART(SPLIT_PART(dc.address_details, ',', 3), ' ', 2) AS Region,
    CASE 
        WHEN od.order_source IN ('website', 'mobile_app') THEN 'Online'
        ELSE 'In-Store'
    END AS SalesChannel,
    dc.segment AS CustomerSegmentImpact
FROM DBT_SAIFSHAHUL_NEW_DATA.STG_ORDERS AS od
JOIN DBT_SAIFSHAHUL_NEW_DATA.dim_product AS dp ON od.product_id = dp.product_id
JOIN DBT_SAIFSHAHUL_NEW_DATA.dim_customer AS dc ON od.customer_id = dc.customer_id
GROUP BY 
    od.order_id, 
    od.customer_id, 
    dp.product_id, 
    od.store_id, 
    od.employee_id, 
    od.campaign_id, 
    od.order_date,
    SPLIT_PART(SPLIT_PART(dc.address_details, ',', 3), ' ', 2) , 
    CASE 
        WHEN od.order_source IN ('website', 'mobile_app') THEN 'Online'
        ELSE 'In-Store'
    END, 
    dc.segment
