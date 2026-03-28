# Flatten-the-Stack

Project Overview

This project performs a comprehensive product sales analysis on semi-structured JSON order data.
By transforming denormalized data into a structured relational format, 
this pipeline enables deep insights into fulfillment performance and order-level revenue.

The Challenge
The source dataset contains a denormalized column (line_items) where multiple products per order are stored as a JSON array. 
This structure is unsuitable for standard relational analysis and requires flattening to achieve First Normal Form (1NF).

The Solution
I implemented a scalable ELT (Extract, Load, Transform) pattern to process the data through the following stages:

1. Data Ingestion
Staging: Raw CSV data is loaded into a staging table (sales_orders).

Preprocessing: Handles string-to-date formatting during the import process to ensure temporal accuracy.

2. Data Normalization
JSON Parsing: Utilizes the JSON_TABLE() function to unpack nested arrays.

Granularity: Converts each nested product within an order into its own unique row.

Data Cleaning: Cleans malformed JSON strings caused by CSV escaping issues to produce a high-integrity table (normalized_sales_orders).

3. Data Modeling & AggregationLogic Design:
Builds reusable Common Table Expressions (CTEs) to aggregate data at the order level.Calculation:
Computes total sales per order ($Price \times Quantity$) and a grand total for efficiency.

4. Analytical Output
Channel Performance: Calculates total sales, order counts, and percent contribution by fulfillment channel (e.g., In-store vs. Online).

High-Value Targets: Identifies top-performing orders by revenue for targeted business analysis.

Key Techniques Demonstrated
Advanced SQL: JSON parsing with JSON_TABLE() and multi-level aggregation using CTEs.

Data Engineering: Designing a robust ELT pipeline (Raw → Normalize → Aggregate → Analyze).

Data Integrity: Handling malformed JSON and ensuring relational compliance.

Optimization: Efficient query design that avoids redundant computations.


