-- ==================================================
-- STEP 1: CREATE STAGING TABLE (RAW INGESTION LAYER)
-- ==================================================
-- Stores raw data exactly as received from the CSV file.
-- The `line_items` column contains JSON arrays stored as text.
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
-- Includes a foreign key to enforce referential integrity.
CREATE TABLE IF NOT EXISTS normalized_sales_orders
(
	order_number INT,
	fulfillment VARCHAR(50),
	product_name VARCHAR(100),
	product_price DECIMAL(10, 2),
	quantity INT,
	FOREIGN KEY (order_number) REFERENCES sales_orders(order_number)
);

-- ==============================
-- STEP 3: LOAD RAW DATA FROM CSV
-- ==============================
-- Handles:
-- - CSV ingestion
-- - Proper parsing of quoted fields
-- - Conversion of string dates into DATE format
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_orders.csv'
INTO TABLE sales_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES
(order_number, @var_date, line_items, fulfillment)
SET order_date = STR_TO_DATE(@var_date, '%m/%d/%Y');

-- =================================================
-- STEP 4: NORMALIZE JSON DATA (CORE TRANSFORMATION)
-- =================================================
-- This step converts semi-structured JSON data into a
-- relational format suitable for analysis.
-- -------------------------------------------------------------
-- Key operations:
-- 1. Clean malformed JSON caused by CSV escaping:
--    - Remove outer quotes
--    - Replace double-double quotes ("") with proper quotes (")
-- 2. Use JSON_TABLE() to:
--    - Expand each JSON array into multiple rows
--    - Extract nested product attributes
-- -------------------------------------------------------------
-- Result:
-- Each product within an order becomes its own row.
-- The WHERE NOT EXISTS clause ensures idempotency,
-- preventing duplicate inserts if the script is re-run.
-- -------------------------------------------------------------
INSERT INTO normalized_sales_orders
SELECT
	s.order_number,
	TRIM(s.fulfillment) AS fulfillment,
	jt.product_name,
	jt.product_price,
	jt.quantity
FROM sales_orders s
CROSS JOIN JSON_TABLE(
	REPLACE(TRIM(BOTH '"' FROM s.line_items), '""', '"'),
	'$[*]' COLUMNS(
	product_name VARCHAR(100) PATH '$.product.product_name',
	product_price DECIMAL(10, 2) PATH '$.product.product_price',
	quantity INT PATH '$.quantity'
	)
) jt
WHERE NOT EXISTS (
	SELECT 1
	FROM normalized_sales_orders n
	WHERE n.order_number = s.order_number AND
		  n.product_name = jt.product_name
);

-- ======================
-- STEP 5: ORDER ANALYSIS
-- ======================
-- -------------------------------------------
-- Objective:
-- Identify high-performing individual orders.
-- -------------------------------------------
-- Metrics:
-- - Number of distinct products per order
-- - Total number of products per order
-- - Total revenue per order
-- -------------------------------------------
SELECT
	order_number,
	fulfillment,
	COUNT(*) AS num_products,
	ROUND(SUM(product_price*quantity)) AS total_sales
FROM normalized_sales_orders
GROUP BY
	order_number,
    fulfillment
ORDER BY total_sales DESC;

-- ===========================
-- STEP 6: MoM GROWTH ANALYSIS
-- ===========================
-- ----------------------------------------
-- Objective:
-- Analyze month-over-month revenue trends.
-- ----------------------------------------
-- Challenge:
-- The dataset may not contain all months.
-- Solution:
-- Generate a continuous calendar and join actual sales data.
WITH RECURSIVE calendar_cte AS
-- ------------------------------------------------------
-- CTE Objective: Generate continuous monthly date range
-- ------------------------------------------------------
(
	SELECT
		MIN(DATE_FORMAT(order_date, '%Y-%m-01')) AS sales_month,
		MAX(DATE_FORMAT(order_date, '%Y-%m-01')) AS max_month
	FROM sales_orders

	UNION ALL

	SELECT
		sales_month + INTERVAL 1 MONTH,
        max_month
	FROM calendar_cte
	WHERE sales_month < max_month
),
actual_sales_cte AS
-- ----------------------------------------------
-- CTE Objective: Aggregate actual sales by month
-- ----------------------------------------------
(
	SELECT
		DATE_FORMAT(s.order_date, '%Y-%m-01') AS sales_month,
		SUM(n.product_price * n.quantity) AS total_sales
	FROM sales_orders s
	INNER JOIN normalized_sales_orders n ON n.order_number = s.order_number
	GROUP BY sales_month
)
-- ------------------------------------------------------------
-- Final Output: Combine calendar with actual sales (fill gaps)
-- ------------------------------------------------------------
SELECT
	YEAR(c.sales_month) AS order_year,
	DATE_FORMAT(c.sales_month, '%b') AS order_month,
	ROUND(COALESCE(a.total_sales, 0)) AS total_sales
FROM calendar_cte c
LEFT JOIN actual_sales_cte a ON c.sales_month = a.sales_month
ORDER BY
	order_year,
    c.sales_month;
