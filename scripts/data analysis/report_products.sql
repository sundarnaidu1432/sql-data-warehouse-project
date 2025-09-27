/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE OR ALTER VIEW gold.report_products AS
WITH detail AS(
SELECT
p.product_key,
p.product_name,
p.category,
p.sub_category,
p.cost,
f.customer_key,
f.order_number,
f.sales_amount,
f.quantity,
f.order_date
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
  
),
aggregation AS( 
SELECT
product_key,
product_name,
category,
sub_category,
cost,
COUNT(DISTINCT customer_key) total_customers,
SUM(sales_amount) total_sales,
COUNT(DISTINCT order_number) total_orders,
SUM(quantity) total_quantity, 
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) lifespan,
MAX(order_date) last_sales
FROM detail
GROUP BY product_key,
product_name,
category,
sub_category,
cost 
)
  
SELECT
product_key,
product_name,
category,
sub_category,
CASE
	WHEN total_sales > 50000 THEN 'High performer'
	WHEN total_sales > 10000 THEN 'Medim performer'
	ELSE 'Low performer'
	END AS product_segment,
cost,
total_customers,
total_sales,
total_orders,
total_quantity, 
lifespan,
DATEDIFF(MONTH,last_sales,GETDATE()) AS recency,
total_sales/total_orders AS AOR, 
total_sales/lifespan AS avg_month_revenue
FROM aggregation
