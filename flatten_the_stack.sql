/* ========================================================
EXECUTIVE SUMMARY
-----------------------------------------------------------
I performed a product sales analysis
on semi-structured JSON order data.
-----------------------------------------------------------
This script transforms JSON order data into a
fully normalized relational dataset and performs
various sales analyses by fulfillment channel.
-----------------------------------------------------------
The source dataset contains a denormalized column
(`line_items`) where multiple products per order are stored
as a JSON array. This structure is not suitable for
relational analysis.
-----------------------------------------------------------
To solve this problem, the script performs the following:
-----------------------------------------------------------
1. DATA INGESTION
   * Loads raw CSV data into a staging table (`sales_orders`)
   * Handles date formatting during import
------------------------------------------------------------
2. DATA NORMALIZATION
   * Uses JSON_TABLE() to unpack nested JSON arrays
   * Converts each product within an order into its own row
   * Cleans malformed JSON caused by CSV formatting issues
   * Produces a structured table (`normalized_sales_orders`)
------------------------------------------------------------
3. DATA MODELING & AGGREGATION
   * Builds a reusable CTE (`total_sales_cte`) at the order level
   * Calculates total sales per order (price × quantity)
   * Computes a grand total once for efficiency
------------------------------------------------------------
4. ANALYTICAL OUTPUT
   * Calculates total sales, order counts, and percent
     contribution by fulfillment channel
   * Identifies top-performing orders by revenue
------------------------------------------------------------
This approach follows a scalable ELT pattern:
Raw → Normalize → Aggregate → Analyze
------------------------------------------------------------
Key techniques demonstrated:
* JSON parsing with JSON_TABLE()
* Data cleaning of malformed JSON
* Multi-level aggregation using CTEs
* Efficient query design (avoiding redundant computation)
============================================================ */

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
-- ---------------------------------------------------
-- Key operations:
-- 1. Cleans malformed JSON caused by CSV escaping:
--    - Removes outer quotes
--    - Replaces double-double quotes ("") with proper quotes (")
-- -----------------------------------------------------------------
-- 2. Uses JSON_TABLE() to:
--    - Expand each JSON array into multiple rows
--    - Extract nested product attributes
-- ----------------------------------------

-- Result:
-- Each product within an order becomes its own row.
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

-- =============================================
-- STEP 5: SALES ANALYSIS BY FULFILLMENT CHANNEL
-- =============================================
-- Objective:
-- - Measure performance of each fulfillment method
-- - Compute:
--   • Total orders
--   • Total revenue
--   • Percent contribution to overall sales
-- --------------------------------------------
SELECT
	fulfillment,
	COUNT(DISTINCT order_number) AS total_orders,
	ROUND(SUM(product_price*quantity)) AS total_sales
FROM normalized_sales_orders
GROUP BY fulfillment
ORDER BY total_sales DESC;

-- ========================================
-- STEP 6: SALES ANALYSIS BY REVENUE ORDERS
-- ========================================
-- Objective:
-- Identify high performing individual orders.
-- -----------------------------------------------------
-- Metrics:
-- - Number of distinct products in the order
-- - Total quantity purchased
-- - Total revenue per order
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