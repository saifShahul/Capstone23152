WITH campaign_sales AS (
    SELECT
        dm.campaign_key,
        fs.CustomerKey,
        fs.order_date,
        fs.TotalSalesAmount,

        -- First purchase date per customer overall
        MIN(fs.order_date) OVER (PARTITION BY fs.CustomerKey) AS first_purchase_date,

        dm.end_date

    FROM {{ ref('facts_sales') }} fs
    JOIN {{ ref('dim_marketingcampaign') }} dm
      ON fs.CampaignKey = dm.campaign_key
    WHERE fs.order_date BETWEEN dm.start_date AND dm.end_date
),

-- Total Sales Influenced by Campaign
sales_influenced AS (
    SELECT
        campaign_key,
        SUM(TotalSalesAmount) AS total_sales_influenced
    FROM campaign_sales
    GROUP BY campaign_key
),

-- New Customers Acquired (first purchase during campaign)
new_customers AS (
    SELECT
        campaign_key,
        COUNT(DISTINCT CustomerKey) AS new_customers_acquired
    FROM campaign_sales
    WHERE order_date = first_purchase_date
    GROUP BY campaign_key
),

-- Repeat Purchasers: customers who purchased during campaign AND again after campaign end date
repeat_purchasers AS (
    SELECT DISTINCT
        cs.campaign_key,
        cs.CustomerKey
    FROM campaign_sales cs
    JOIN {{ ref('facts_sales') }} fs2
      ON cs.CustomerKey = fs2.CustomerKey
    JOIN {{ ref('dim_marketingcampaign') }} dm2
      ON cs.campaign_key = dm2.campaign_key
    WHERE fs2.order_date > dm2.end_date
),

-- Repeat metrics: count first time customers & repeat purchasers
repeat_metrics AS (
    SELECT
        cs.campaign_key,
        COUNT(DISTINCT CASE WHEN cs.order_date = cs.first_purchase_date THEN cs.CustomerKey END) AS first_time_customers,
        COUNT(DISTINCT rp.CustomerKey) AS repeat_purchasers
    FROM campaign_sales cs
    LEFT JOIN repeat_purchasers rp ON cs.campaign_key = rp.campaign_key AND cs.CustomerKey = rp.CustomerKey
    GROUP BY cs.campaign_key
)

SELECT
    dm.campaign_key,
    dm.start_date,
    dm.end_date,
    dm.budget,

    COALESCE(si.total_sales_influenced,0) AS total_sales_influenced,
    COALESCE(nc.new_customers_acquired,0) AS new_customers_acquired,

    /*CASE 
        WHEN rm.first_time_customers = 0 THEN 0
        ELSE ROUND((rm.repeat_purchasers * 100.0) / rm.first_time_customers, 2)
    END AS repeat_purchase_rate,*/

    CASE 
        WHEN dm.budget = 0 THEN NULL
        ELSE -(ROUND(((COALESCE(si.total_sales_influenced,0) - dm.budget) / dm.budget) * 100, 2))
    END AS roi_metrics

FROM {{ ref('dim_marketingcampaign') }} dm
LEFT JOIN sales_influenced si ON dm.campaign_key = si.campaign_key
LEFT JOIN new_customers nc ON dm.campaign_key = nc.campaign_key
LEFT JOIN repeat_metrics rm ON dm.campaign_key = rm.campaign_key

ORDER BY dm.campaign_key

