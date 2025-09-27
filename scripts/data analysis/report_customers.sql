/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	  2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	   - recency (months since last order)
		 - average order value
		 - average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS 
WITH details as(
SELECT 
C.customer_key,
c.customer_number,
CONCAT(C.first_name,' ',C.last_name) full_name,
DATEDIFF(YEAR,C.birth_date,GETDATE()) age,
f.order_date,
f.order_number,
f.product_key,
quantity,
f.sales_amount
FROM      gold.dim_customers c
LEFT JOIN gold.fact_sales f
ON        f.customer_key = c.customer_key
WHERE f.order_date IS NOT NULL
),
aggregated as(
SELECT
customer_key,
full_name,
customer_number,
age,
COUNT(DISTINCT order_number) total_orders,
SUM(sales_amount) total_sales,
COUNT(DISTINCT product_key) total_products,
SUM(quantity) total_quantity,
MAX(order_date) AS last_order,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan

FROM details
GROUP BY customer_key,
full_name,
customer_number,
age)

SELECT 
customer_key,
full_name,
customer_number,
age,
CASE
	WHEN age < 20  THEN 'Under 20'
	WHEN age BETWEEN 20 AND 29 THEN '20-29'
	WHEN age BETWEEN 30 AND 39 THEN '30-39'
	WHEN age BETWEEN 40 AND 49 THEN '40-49'
	ELSE '50 and above'
	END AS age_group,
CASE
	WHEN total_sales > 5000 AND lifespan >=12 THEN 'VIP'
	WHEN total_sales <= 5000 AND lifespan >= 12 THEN 'Regular'
	ELSE 'NEW'
	END AS categories,
total_orders,
total_sales,
total_products,
total_quantity,
(total_sales/total_orders) AS avg_order_value,
CASE
	WHEN lifespan = 0 THEN total_sales
	ELSE total_sales/lifespan
	END AS avg_month_spens,
DATEDIFF(MONTH,last_order,GETDATE()) AS recency,
lifespan
FROM aggregated
