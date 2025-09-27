/*
===============================================================================
category_percentage by year
===============================================================================
Purpose:
    - To compare performance or metrics across dimensions or time periods.
    - To evaluate differences between categories.
    - Useful for A/B testing or regional comparisons.

SQL Functions Used:
    - SUM(): Aggregates values for comparison.
    - Window Functions: SUM() OVER() for total calculations.
===============================================================================
*/
-- Which categories contribute the most to overall sales by year ?
SELECT
year(order_date) AS dat,
p.category,
SUM(f.sales_amount) AS cat_total,
SUM(SUM(f.sales_amount)) over() AS total,
CONCAT(ROUND(CAST(SUM(f.sales_amount)AS FLOAT) / SUM(SUM(f.sales_amount)) over()*100,2),'%') AS percentage_of_total
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE order_date IS NOT NULL
GROUP BY p.category,year(order_date)
ORDER BY dat
