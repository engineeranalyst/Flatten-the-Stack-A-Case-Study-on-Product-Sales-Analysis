/* =========================================================
EXECUTIVE SUMMARY
------------------------------------------------------------
This project performs a comprehensive product sales analysis
on semi-structured JSON order data using Excel and MySQL.
------------------------------------------------------------
The dataset contains a denormalized structure where multiple
products per order are stored as a JSON array, making it
unsuitable for direct relational analysis.
-------------------------------------------------------------
This script transforms the raw data into a structured format,
builds a scalable data model, and performs analytical queries
to generate business insights across orders, fulfillment
channels, and time.
--------------------------------------------------------------
## PROJECT OBJECTIVES
--------------------------------------------------------------
* Convert semi-structured JSON data into a relational format
* Build a reusable and scalable data transformation pipeline
* Analyze product sales performance across multiple dimensions
* Identify revenue trends and high-performing orders
--------------------------------------------------------------
## DATA PIPELINE OVERVIEW (ELT)
--------------------------------------------------------------
1. DATA INGESTION
   * Load raw CSV data into a staging table (`sales_orders`)
   * Convert string dates into proper DATE format
--------------------------------------------------------------
2. DATA NORMALIZATION
   * Use JSON_TABLE() to unpack nested JSON arrays
   * Convert each product into its own row (1NF compliance)
   * Clean malformed JSON caused by CSV formatting issues
---------------------------------------------------------------
3. DATA MODELING
   * Create a normalized fact table (`normalized_sales_orders`)
   * Enforce referential integrity via foreign keys
   * Ensure pipeline re-runnability using idempotent logic
---------------------------------------------------------------
4. ANALYTICAL LAYER
   * Perform order-level revenue analysis
   * Analyze product and fulfillment performance
--------------------------------------------------------------
## KEY TECHNIQUES DEMONSTRATED
--------------------------------------------------------------
* JSON parsing using JSON_TABLE()
* Data cleaning and transformation of malformed JSON
* Relational data modeling with foreign key constraints
* Idempotent data loading (duplicate prevention)
* Multi-level aggregation and analytical SQL patterns
--------------------------------------------------------------
## KEY ANALYSES PERFORMED
--------------------------------------------------------------
* Order-Level Analysis
  - Total revenue per order
  - Product diversity and quantity per order
--------------------------------------------------------------
* Fulfillment Channel Analysis
  - Product Performance by by fulfillment method
--------------------------------------------------------------
## ARCHITECTURE PATTERN
--------------------------------------------------------------
* Raw → Normalize → Model → Analyze
* This mirrors real-world data workflows used in:
  data warehouses, ETL pipelines, and BI systems.
--------------------------------------------------------------
## DATA VISUALIZATION
--------------------------------------------------------------
* I used Excel to build an interactive dashboard
  that summarizes the key insights and
  data limitations found in my analysis
  of this project.
============================================================= */

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