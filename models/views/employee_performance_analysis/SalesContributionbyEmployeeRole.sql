{{ config(materialized = 'view') }}

SELECT
    de.role AS employee_role,
    COUNT(DISTINCT fs.EmployeeKey) AS number_of_employees,
    SUM(fs.TotalSalesAmount) AS total_sales,
    ROUND(100.0 * SUM(fs.TotalSalesAmount) / SUM(SUM(fs.TotalSalesAmount)) OVER (), 2) AS sales_contribution_percentage
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_employee') }} AS de
    ON fs.EmployeeKey = de.employee_id
GROUP BY de.role
ORDER BY total_sales DESC