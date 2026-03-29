-- ==================================================
-- STEP 1: CREATE STAGING TABLE (RAW INGESTION LAYER)
-- ==================================================
-- This table stores raw data exactly as received from the CSV.
-- The `line_items` column contains JSON arrays as strings.
CREATE TABLE IF NOT EXISTS sales_orders
(
	order_number INT,
	order_date DATE,
	line_items VARCHAR(2000),
	fulfillment VARCHAR(20),
	PRIMARY KEY (order_number)
);

-- ===================================================
-- STEP 2: CREATE NORMALIZED TABLE (TRANSFORMED LAYER)
-- ===================================================
-- This table represents a fully normalized structure:
-- One row per product per order (1NF compliant).
CREATE TABLE IF NOT EXISTS normalized_sales_orders
(
	order_number INT,
	order_date DATE,
	fulfillment VARCHAR(50),
	product_name VARCHAR(100),
	product_price DECIMAL(10, 2),
	quantity INT
);

-- ==============================
-- STEP 3: LOAD RAW DATA FROM CSV
-- ==============================
-- Handles:
-- - CSV ingestion
-- - Date conversion from string to DATE format
-- - Proper parsing of quoted fields
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_orders.csv'
INTO TABLE sales_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES -- Skip header row
(order_number, @var_date, line_items, fulfillment)
SET order_date = STR_TO_DATE(@var_date, '%m/%d/%Y'); -- Put the dates in the proper format

-- =================================================
-- STEP 4: NORMALIZE JSON DATA (CORE TRANSFORMATION)
-- =================================================
-- This is the most important step in the pipeline.
-- -----------------------------------------------------------------
-- Key operations:
-- 1. Cleans malformed JSON caused by CSV escaping:
--    - Removes outer quotes
--    - Replaces double-double quotes ("") with proper quotes (")
-- -----------------------------------------------------------------
-- 2. Uses JSON_TABLE() to:
--    - Expand each JSON array into multiple rows
--    - Extract nested product attributes
-- -----------------------------------------------------------------
-- Result:
-- Each product within an order becomes its own row.
-- -----------------------------------------------------------------
INSERT INTO normalized_sales_orders
SELECT
	o.order_number,
	o.order_date,
	TRIM(o.fulfillment) AS fulfillment, -- Clean trailing whitespace
	jt.product_name,
	jt.product_price,
	jt.quantity
FROM sales_orders o
CROSS JOIN JSON_TABLE(
	-- 1. Remove the triple quotes from the start and end
    -- 2. Replace the double-double quotes ("") with single-double quotes (")
	REPLACE(TRIM(BOTH '"' FROM o.line_items), '""', '"'),
		'$[*]' COLUMNS(
		product_name VARCHAR(100) PATH '$.product.product_name',
		product_price DECIMAL(10, 2) PATH '$.product.product_price',
		quantity INT PATH '$.quantity'
	)
) jt
WHERE NOT EXISTS (
-- Code to prevent this statement from running if records already exist in the table.
    SELECT 1 
    FROM normalized_sales_orders n 
    WHERE n.order_number = o.order_number AND
		  n.product_name = jt.product_name
);

-- ======================================
-- STEP 5: SALES ANALYSIS BY FULFILLMENTS
-- ======================================
-- Objective:
-- - Measure performance of each fulfillment method
-- ------------------------------------------------
-- - Compute:
--   • Total orders
--   • Total revenue
--   • Percent contribution to overall sales
-- ------------------------------------------------
SELECT
	fulfillment,
	COUNT(DISTINCT order_number) AS total_orders,
	ROUND(SUM(product_price*quantity)) AS total_sales,
    ROUND(SUM(product_price*quantity)/COUNT(DISTINCT order_number)) AS average_order_value
FROM normalized_sales_orders
GROUP BY fulfillment
ORDER BY total_sales DESC;

-- ================================
-- STEP 6: SALES ANALYSIS BY ORDERS
-- ================================
-- Objective:
-- Identify high performing individual orders.
-- -------------------------------------------
-- Metrics:
-- - Number of distinct products in the order
-- - Total quantity purchased
-- - Total revenue per order
-- -------------------------------------------
SELECT
	order_number,
	fulfillment,
    COUNT(*) AS num_products,
	SUM(quantity) AS total_quantity_purchased,
	ROUND(SUM(product_price*quantity)) AS total_sales
FROM normalized_sales_orders
GROUP BY
	order_number,
	fulfillment
ORDER BY total_sales DESC;

-- ===========================
-- STEP 7: MoM GROWTH ANALYSIS
-- ===========================
-- Objective:
-- Identify Month-over-Month growth performance.
-- -------------------------------------------------------
-- Caveats:
-- - The years in the dataset don't have all of the months
-- - This query accounts for that problem
-- -------------------------------------------------------
-- STEP 1: Generate a continuous list of all months in your data range
WITH RECURSIVE calendar_cte AS 
(
    SELECT
		MIN(DATE_FORMAT(order_date, '%Y-%m-01')) AS sales_month,
		MAX(DATE_FORMAT(order_date, '%Y-%m-01')) AS max_month
    FROM normalized_sales_orders
    
    UNION ALL
    
    SELECT sales_month + INTERVAL 1 MONTH, max_month
    FROM calendar_cte
    WHERE sales_month < max_month
),
-- STEP 2: Calculate actual sales from your data
actual_sales_cte AS 
(
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m-01') AS sales_month,
        SUM(product_price*quantity) AS total_sales
    FROM normalized_sales_orders
    GROUP BY sales_month
)
-- STEP 3: Join them together to fill the gaps
SELECT 
    YEAR(c.sales_month) AS year,
    DATE_FORMAT(c.sales_month, '%b') AS month,
    ROUND(a.total_sales) AS total_sales
FROM calendar_cte c
LEFT JOIN actual_sales_cte a ON c.sales_month = a.sales_month
ORDER BY c.sales_month;
