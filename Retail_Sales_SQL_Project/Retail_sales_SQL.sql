-- SQL Retail Sales Analysis Questionnaire

-- Intermediate Level Questions

-- 1. What are the top 5 selling products by total revenue?
SELECT category, SUM(total_sale) as total_revenue
FROM retail_sales
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 5;

-- 2. Calculate the average transaction value by gender.
SELECT gender, AVG(total_sale) as avg_transaction_value
FROM retail_sales
GROUP BY gender;

-- 3. Find the busiest hour of the day in terms of number of transactions.
SELECT EXTRACT(HOUR FROM sale_time) as hour_of_day, COUNT(*) as transaction_count
FROM retail_sales
GROUP BY hour_of_day
ORDER BY transaction_count DESC
LIMIT 1;

-- 4. Calculate the month-over-month growth rate of total sales.
WITH monthly_sales AS (
  SELECT 
    DATE_TRUNC('month', sale_date) as month,
    SUM(total_sale) as total_sales
  FROM retail_sales
  GROUP BY month
)
SELECT 
  month,
  total_sales,
  LAG(total_sales) OVER (ORDER BY month) as prev_month_sales,
  (total_sales - LAG(total_sales) OVER (ORDER BY month)) / LAG(total_sales) OVER (ORDER BY month) * 100 as growth_rate
FROM monthly_sales
ORDER BY month;

-- 5. Identify customers who have made purchases in all product categories.
SELECT customer_id
FROM (
  SELECT customer_id, COUNT(DISTINCT category) as category_count
  FROM retail_sales
  GROUP BY customer_id
) as customer_categories
WHERE category_count = (SELECT COUNT(DISTINCT category) FROM retail_sales);

-- Advanced Level Questions

-- 6. Calculate the rolling 7-day average of daily sales.
WITH daily_sales AS (
  SELECT sale_date, SUM(total_sale) as daily_total
  FROM retail_sales
  GROUP BY sale_date
)
SELECT 
  sale_date,
  daily_total,
  AVG(daily_total) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_avg
FROM daily_sales
ORDER BY sale_date;

-- 7. Identify the top 5% of customers by total spend and their preferred product category.
WITH customer_totals AS (
  SELECT 
    customer_id, 
    SUM(total_sale) as total_spend,
    PERCENT_RANK() OVER (ORDER BY SUM(total_sale) DESC) as spend_percentile
  FROM retail_sales
  GROUP BY customer_id
),
customer_preferences AS (
  SELECT 
    customer_id, 
    category,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) as category_rank
  FROM retail_sales
  GROUP BY customer_id, category
)
SELECT 
  ct.customer_id,
  ct.total_spend,
  cp.category as preferred_category
FROM customer_totals ct
JOIN customer_preferences cp ON ct.customer_id = cp.customer_id AND cp.category_rank = 1
WHERE ct.spend_percentile <= 0.05
ORDER BY ct.total_spend DESC;

-- 8. Calculate the customer retention rate month-over-month.
WITH monthly_active_customers AS (
  SELECT DATE_TRUNC('month', sale_date) as month, customer_id
  FROM retail_sales
  GROUP BY month, customer_id
),
customer_retention AS (
  SELECT 
    current_month.month,
    COUNT(DISTINCT current_month.customer_id) as total_customers,
    COUNT(DISTINCT previous_month.customer_id) as retained_customers
  FROM monthly_active_customers current_month
  LEFT JOIN monthly_active_customers previous_month 
    ON current_month.customer_id = previous_month.customer_id
    AND current_month.month = previous_month.month + INTERVAL '1 month'
  GROUP BY current_month.month
)
SELECT 
  month,
  total_customers,
  retained_customers,
  COALESCE(retained_customers::float / NULLIF(LAG(total_customers) OVER (ORDER BY month), 0) * 100, 0) as retention_rate
FROM customer_retention
ORDER BY month;

-- 9. Identify products that are often purchased together (market basket analysis).
SELECT 
  r1.category as category1,
  r2.category as category2,
  COUNT(*) as co_occurrence,
  COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT transactions_id) FROM retail_sales) as percentage
FROM retail_sales r1
JOIN retail_sales r2 ON r1.transactions_id = r2.transactions_id AND r1.category < r2.category
GROUP BY r1.category, r2.category
HAVING COUNT(*) > 10
ORDER BY co_occurrence DESC
LIMIT 5;

-- 10. Calculate the Customer Lifetime Value (CLV) and segment customers.
WITH customer_metrics AS (
  SELECT 
    customer_id,
    SUM(total_sale) as total_revenue,
    COUNT(DISTINCT transactions_id) as frequency,
    (MAX(sale_date) - MIN(sale_date)) / 365.0 as customer_lifespan_years,
    (CURRENT_DATE - MAX(sale_date)) / 365.0 as years_since_last_purchase
  FROM retail_sales
  GROUP BY customer_id
),
clv_calculation AS (
  SELECT 
    customer_id,
    total_revenue,
    frequency,
    customer_lifespan_years,
    years_since_last_purchase,
    CASE 
      WHEN customer_lifespan_years > 0 THEN (total_revenue / customer_lifespan_years) * (1 / (1 + years_since_last_purchase)) * 5
      ELSE total_revenue
    END as estimated_clv
  FROM customer_metrics
)
SELECT 
  customer_id,
  estimated_clv,
  CASE 
    WHEN estimated_clv > PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY estimated_clv) THEN 'High Value'
    WHEN estimated_clv > PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimated_clv) THEN 'Medium Value'
    ELSE 'Low Value'
  END as customer_segment
FROM clv_calculation
ORDER BY estimated_clv DESC;