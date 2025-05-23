{{ config(materialized = 'view') }}

SELECT
    de.employee_id,
    de.full_name,
    de.role,
    de.tenure_years,
    fs.Region,
    COUNT(DISTINCT fs.OrderID) AS total_orders_handled,
    SUM(fs.TotalSalesAmount) AS total_sales,
    SUM(fs.TotalSalesAmount) / NULLIF(de.tenure_years, 0) AS avg_sales_per_year
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_employee') }} AS de
    ON fs.EmployeeKey = de.employee_id
GROUP BY de.employee_id, de.full_name, de.role, de.tenure_years, fs.Region