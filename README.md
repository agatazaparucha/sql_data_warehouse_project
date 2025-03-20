# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository!  
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering, ETL processes, and analytics.

---

## Data Architecture

The data architecture for this project follows the **Medallion Architecture** (Bronze, Silver, and Gold layers), ensuring a scalable and maintainable data pipeline:

![Medallion Architecture](https://github.com/user-attachments/assets/65e7a2da-3bbf-4206-a468-66e8e12153b1)

### Layers Overview:
1. **Bronze Layer**:  
   - Stores raw data as-is from the source systems.  
   - Data is ingested from CSV files into a SQL Server database.  
   - No transformations are applied at this stage.  

2. **Silver Layer**:  
   - Includes data cleansing, standardization, and normalization processes.  
   - Handles data quality issues (e.g., null values, duplicates, invalid formats).  
   - Prepares data for analysis by enriching and transforming it.  

3. **Gold Layer**:  
   - Houses business-ready data modeled into a **star schema** for reporting and analytics.  
   - Includes dimension tables (`dim_customers`, `dim_products`) and a fact table (`fact_sales`).  
   - Optimized for fast querying and generating actionable insights.  

---

## Project Overview

This project involves the following key components:

### 1. **Data Ingestion**
   - Raw data is ingested from CSV files into the **Bronze Layer** of the data warehouse.  
   - Source systems include CRM and ERP data (e.g., customer information, product details, sales transactions).  

### 2. **ETL Pipelines**
   - **Extract, Transform, Load (ETL)** processes are implemented to move data from the Bronze Layer to the Silver and Gold layers.  
   - Transformations include:  
     - Cleaning and standardizing data (e.g., handling nulls, normalizing values).  
     - Joining tables to enrich data (e.g., customer and location data).  
     - Applying business logic (e.g., calculating valid sales prices, correcting date ranges).  

### 3. **Data Modeling**
   - The **Gold Layer** is designed using a **star schema** for optimal analytical performance.  
   - **Dimension Tables**:  
     - `dim_customers`: Contains customer details (e.g., name, gender, marital status, country).  
     - `dim_products`: Contains product details (e.g., name, category, cost, product line).  
   - **Fact Table**:  
     - `fact_sales`: Contains sales transaction data (e.g., order number, product key, customer key, sales amount, quantity, price).  

### 4. **Analytics & Reporting**
   - SQL-based queries are used to generate reports and insights from the Gold Layer.  
   - Example insights:  
     - Sales performance by product category.  
     - Customer demographics and purchasing behavior.  
     - Revenue trends over time.  

---

## Key Features

- **Data Quality Checks**: Ensures data integrity by handling nulls, duplicates, and invalid values.  
- **Surrogate Keys**: Uses `ROW_NUMBER()` to create unique surrogate keys for dimension tables.  
- **Data Enrichment**: Joins multiple tables to provide a comprehensive view of the data.  
- **Star Schema**: Optimizes query performance for analytical workloads.  
- **Error Handling**: Includes robust error handling in ETL processes to ensure data consistency.  

---

## Getting Started

### Prerequisites
- **SQL Server**: Ensure you have SQL Server installed and running.  
- **CSV Files**: Place the source CSV files in the `datasets/` directory.  

### Steps to Run the Project
1. **Ingest Data**: Run the Bronze Layer scripts to load raw data into the database.  
2. **Transform Data**: Execute the Silver Layer scripts to clean and standardize the data.  
3. **Model Data**: Run the Gold Layer scripts to create dimension and fact tables.  
4. **Analyze the Data**: Analyze the data using the views created in the gold layer.  

---

## Example Queries

### Sales Performance by Product Category
```sql
SELECT 
    p.category,
    SUM(f.sls_sales) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;
```
### Customer Demographic
```sql
SELECT 
    c.gender,
    c.marital_status,
    COUNT(*) AS customer_count
FROM gold.dim_customers c
GROUP BY c.gender, c.marital_status;
```
