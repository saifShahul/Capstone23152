{{ config(materialized = 'view') }}

SELECT
    fs.Region,
    fs.EmployeeKey AS employee_id,
    de.full_name,
    de.role,
    SUM(fs.TotalSalesAmount) AS total_sales,
    RANK() OVER (PARTITION BY fs.Region ORDER BY SUM(fs.TotalSalesAmount) DESC) AS regional_rank
FROM {{ ref('facts_sales') }} AS fs
JOIN {{ ref('dim_employee') }} AS de
    ON fs.EmployeeKey = de.employee_id
GROUP BY fs.Region, fs.EmployeeKey, de.full_name, de.role