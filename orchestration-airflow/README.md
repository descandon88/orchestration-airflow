# 🛠️ ETL E-commerce Pipeline with Apache Airflow

## 📌 Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** using Apache Airflow to process e-commerce data from multiple sources.

The pipeline ingests raw CSV datasets, performs data transformations and joins, and generates analytical outputs such as revenue metrics and cleaned datasets.

---

## 🎯 Objectives

- Build a production-style ETL pipeline
- Orchestrate workflows using Airflow
- Practice data modeling and transformations
- Ensure basic idempotency and robustness
- Simulate a real-world e-commerce data workflow

---


---

## ⚙️ Engineering Highlights

This project goes beyond a simple ETL by incorporating key **data engineering best practices**:

- → Spun up Airflow with Docker Compose for local development  
- → Migrated standalone scripts into DAG-based workflows with clear dependencies  
- → Defined task dependencies using Airflow operators (`>>`)  
- → Configured retries to handle transient failures (with retry logic in DAGs)  
- → Designed the pipeline to be modular and extensible  
- → Implemented orchestration patterns for scalable workflows  

### 🔜 Planned / Advanced Features

- → Implement Slack alerts for DAG failures  
- → Create a master DAG orchestrating multiple pipelines using `TriggerDagRunOperator`  
- → Add exponential backoff strategy for retries  
- → Introduce data quality validation checks  

---

## 📂 Dataset

The pipeline uses the following datasets:

- `ecommerce_orders.csv`
- `ecommerce_order_items.csv`
- `ecommerce_products.csv`
- `ecommerce_customers.csv`
- `ecommerce_categories.csv`
- `ecommerce_brands.csv`
- `ecommerce_inventory.csv`
- `ecommerce_suppliers.csv`
- `ecommerce_promotions.csv`
- `ecommerce_reviews.csv`
- `ecommerce_warehouses.csv`

---

## ⚙️ Tech Stack

- Python
- Apache Airflow
- Pandas
- Docker & Docker Compose
- PostgreSQL (Airflow metadata DB)

---

## 🔄 Pipeline Description

### 1. Extract
- Validates existence of all required CSV files
- Passes file paths via XCom

### 2. Transform
- Loads datasets into Pandas DataFrames
- Performs joins across:
  - Orders
  - Order Items
  - Products
  - Categories
  - Brands
  - Customers
- Cleans and standardizes data
- Creates derived fields:
  - `order_month`
  - `total` (price * quantity)

### 3. Load
- Saves processed dataset as Parquet
- Generates summary metrics in JSON format

---

## 📊 Output

- `fact_orders.parquet` → Clean analytical dataset  
- `summary.json` → Aggregated metrics including:
  - Total orders
  - Total revenue
  - Average order value
  - Total customers

---

## ▶️ How to Run

### 1. Start services

```bash
docker-compose up -d
