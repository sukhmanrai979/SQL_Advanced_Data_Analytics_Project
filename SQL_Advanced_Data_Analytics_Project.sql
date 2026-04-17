-- Change over time analysis
-- Analyse how a measure evolves over time
-- Helps track trends and identify seasonality in your data
-- [Measure] By [Date Dimension]
-- e.g. Total Sales by year

-- Total Sales, Quantity, Customers by year
SELECT 
YEAR(order_date) OrderYear, 
SUM(sales_amount) TotalSales,
COUNT(DISTINCT customer_key) TotalCustomers,
SUM(quantity) TotalQuantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)


-- Total Sales, Quantity, Customers by Year and month
SELECT 
YEAR(order_date) OrderYear, 
MONTH(order_date) OrderMonth, 
SUM(sales_amount) TotalSales,
COUNT(DISTINCT customer_key) TotalCustomers,
SUM(quantity) TotalQuantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)



-- Cumulative Analysis
-- Aggregating the data progressively over time
-- Helps to understand whether our business is growing or declining
-- [Cumulative Measure] By [Date Dimension]
-- Running total and moving average


-- Running total for total sales per month (partitioned by year)
-- Moving average for average price of items sold per month (partitioned by year)
SELECT order_date,
TotalSales,
SUM(TotalSales) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) RunningTotal,
AVG(AvgPrice) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) MovingAverage
FROM
(
    SELECT 
    DATETRUNC(month, order_date) order_date,
    SUM(sales_amount) TotalSales,
    AVG(price) AvgPrice
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(month, order_date)
)t


-- Performance Analysis
-- Comparing current value to target value
-- Helps measure success and compare performance
-- Current Measure - Target Measure
-- e.g. Current sales vs Average Sales
-- Current year sales vs Previous Year Sales

WITH CTE_Current_Sales AS(
SELECT
product_key,
YEAR(order_date) Year,
SUM(sales_amount) CurrentSales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), product_key
)

SELECT product_key, 
year, 
CurrentSales
FROM CTE_Current_Sales

 
-- Part to Whole Analysis (Propotional Analysis)
-- Analyse how an individual part is performing compared to the overall
-- Allowing us to understand which category has the greatest impact
-- on the business
-- Formula: [Measure]/ Total[Measure] * 100 By Dimension


-- Which categories contribute the most to overall sales
SELECT 
category, 
TotalSales AS TotalSalesByCategory,
SUM(TotalSales) OVER() TotalSalesOverall,
CONCAT(ROUND(CAST(TotalSales AS FLOAT) / SUM(TotalSales) OVER() * 100, 2), '%') PercentageContribution
FROM (
    SELECT dp.category, 
    SUM(fs.sales_amount) TotalSales FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
    GROUP BY dp.category
)t
ORDER BY PercentageContribution DESC

-- Data Segmentation
-- Group the data based on a specific range
-- Helps understand the correlation between two measures
-- [Measure] By [Measure]
-- e.g. Total Products by Sales Range
-- Total Customers by Age

-- Segment products into cost ranges and count how many products fall
-- into each segment

WITH CTE_product_price_categories AS
(SELECT 
product_key, 
product_name, 
cost, 
CASE
    WHEN cost < 700 THEN 'Below 700'
    WHEN cost < 1400 THEN 'Between 700 and 1400'
    ELSE 'Above 1400'
END PriceCategory
FROM gold.dim_products)

SELECT 
PriceCategory, 
COUNT(product_key) TotalProducts
FROM CTE_product_price_categories
GROUP BY PriceCategory
ORDER BY TotalProducts DESC




-- Segmenting customer based on the number of months they have been
-- with the business and the amount of money they have spent
WITH CTE_customer_history_and_spending AS
(
    SELECT 
    customer_key, 
    MIN(order_date) FirstOrder, 
    MAX(order_date) LastOrder,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) lifespan,
    SUM(sales_amount) TotalSpent
    FROM gold.fact_sales
    GROUP BY customer_key
),

CTE_customer_segments AS
(
    SELECT
    customer_key,
    CASE
        WHEN lifespan >= 12 AND TotalSpent > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND TotalSpent <= 5000 THEN 'Regular'
        ELSE 'New'
    END CustomerStatus
    FROM CTE_customer_history_and_spending
)

SELECT 
CustomerStatus, 
COUNT(customer_key) TotalCustomers
FROM CTE_customer_segments
--GROUP BY CustomerStatus


-- Build Customers Report
GO

CREATE VIEW gold.report_customers AS
-- Base Query
WITH base_query AS (
    SELECT 
    fs.order_number, 
    fs.product_key,
    fs.sales_amount,
    fs.order_date,
    fs.quantity,
    dc.customer_key,
    dc.customer_id,
    CONCAT(dc.first_name, ' ', dc.last_name) AS customer_name,
    DATEDIFF(Year, dc.birthdate, GETDATE()) AS age
    FROM gold.fact_sales fs LEFT JOIN
    gold.dim_customers dc ON
    fs.customer_key = dc.customer_key
    WHERE order_date IS NOT NULL
),

customer_aggregations AS
(
SELECT 
    customer_key,
    customer_id,
    customer_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    MAX(order_date) last_order_date,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) lifespan
FROM base_query
GROUP BY
    customer_key,
    customer_id,
    customer_name,
    age
)

SELECT 
    customer_key,
    customer_id,
    customer_name,
    age,
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and above'
    END age_group,
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END customer_segment,
    last_order_date,
    DATEDIFF(month, last_order_date, GETDATE()) recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Compute Average Order Value (AVO)
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE ROUND(CAST(total_sales AS FLOAT) / total_orders, 2) 
    END avg_order_value,

    -- Compute Average Monthly Spend
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE CAST(total_sales AS FLOAT) / lifespan 
    END avg_monthly_spend
    
FROM customer_aggregations


-- Build Products Report
GO

CREATE VIEW gold.report_products AS
WITH product_order_details AS
(
    SELECT 
        dp.product_key,
        dp.product_name, 
        dp.category, 
        dp.subcategory, 
        dp.cost,
        fs.order_number,
        fs.order_date,
        fs.customer_key,
        fs.sales_amount,
        fs.quantity
    FROM gold.fact_sales fs LEFT JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
    WHERE order_date IS NOT NULL
),

product_aggregations AS
(
    SELECT 
        product_key,
        product_name, 
        category, 
        subcategory, 
        cost,
        COUNT(order_number) total_orders,
        SUM(sales_amount) total_sales,
        SUM(quantity) total_quantity,
        COUNT(DISTINCT customer_key) total_customers,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) lifespan,
        MIN(order_date) last_order
    FROM product_order_details
    GROUP BY
        product_key,
        product_name, 
        category, 
        subcategory, 
        cost
)

SELECT 
        product_key,
        product_name, 
        category, 
        subcategory, 
        cost,
        total_orders,
        total_sales,
        total_quantity,
        total_customers,
        lifespan,
        CASE
            WHEN total_sales < 100000 THEN 'Low-Performer'
            WHEN total_sales < 500000 THEN 'Mid-Range Performer'
            ELSE 'High Performer'
        END performance_segment,
        DATEDIFF(month, last_order, GETDATE()) recency,
        CASE
            WHEN total_orders = 0 THEN 0
            ELSE ROUND(CAST(total_sales AS FLOAT) / total_orders, 2) 
        END avg_order_revenue,
        CASE
            WHEN lifespan = 0 THEN total_sales
            ELSE ROUND(CAST(total_sales AS FLOAT) / lifespan, 2)
        END avg_monthly_revenue 
FROM product_aggregations

GO
SELECT * FROM gold.report_products