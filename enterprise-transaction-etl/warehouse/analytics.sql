-- GOLD LAYER ANALYTICS QUERIES

--  TOTAL REVENUE (Executive KPI)
-- Answers: How much money did we make in total?
SELECT 
    ROUND(SUM(revenue), 2) AS total_revenue
FROM fact_sales;


--  REVENUE BY COUNTRY
-- Answers: Which countries generate the most revenue?
SELECT 
    c.country_name,
    ROUND(SUM(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_country c
    ON f.country_key = c.country_key
GROUP BY c.country_name
ORDER BY revenue DESC;


--  MONTHLY REVENUE TREND
-- Answers: How does revenue change over time?
SELECT 
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


--  TOP 10 PRODUCTS BY REVENUE
-- --------------------------------
-- Answers: Which products make the most money?
SELECT 
    p.product_name,
    ROUND(SUM(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;


--  TRANSACTION STATUS BREAKDOWN
-- --------------------------------
-- Answers: How many completed vs cancelled vs returned?
SELECT
    transaction_status,
    COUNT(*) AS transaction_count,
    ROUND(SUM(revenue), 2) AS total_revenue
FROM fact_sales
GROUP BY transaction_status
ORDER BY total_revenue DESC;
