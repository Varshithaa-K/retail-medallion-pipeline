#  Retail Medallion Pipeline

An end-to-end data engineering project built on **Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)**, orchestrated with **Apache Airflow** and visualized with **Tableau**.

---

##  Live Dashboard
1. **Retail Product Insights Dashboard** - https://public.tableau.com/views/RetailProductInsightsDashboard/RetailProductInsightsDashboard?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

2. **Retail Sales Analytics - Medallion Pipeline **- https://public.tableau.com/views/RetailSalesAnalytics-MedallionPipeline/RetailSalesDashboard?:language=en-US&:sid=&:redirect=auth&publish=yes&showOnboarding=true&:display_count=n&:origin=viz_share_link

---

##  Architecture

```
Raw CSV (Kaggle Retail Orders Dataset)
           │
           ▼
┌─────────────────────────────────────────┐
│              BRONZE LAYER               │
│  • Ingests raw CSV as-is                │
│  • Standardizes column names            │
│  • Adds metadata: ingestion_timestamp,  │
│    source_file, pipeline_name           │
│  • Stored as Delta Lake table           │
└─────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│              SILVER LAYER               │
│  • Deduplication via Window functions   │
│  • Casts data types (dates, numerics)   │
│  • Engineers new features:              │
│    - discount_amount                    │
│    - sale_price                         │
│    - profit                             │
│    - profit_margin_pct                  │
│  • Stored as Delta Lake table           │
└─────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│               GOLD LAYER                │
│  • gold_sales_by_region                 │
│    Monthly revenue aggregations         │
│    by region and category               │
│  • gold_top_products                    │
│    Top 10 products per category         │
│    ranked by total profit               │
│  • Analytics-ready Delta tables         │
└─────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│            ORCHESTRATION                │
│  Apache Airflow DAG                     │
│  bronze_ingestion                       │
│       → silver_transformation           │
│       → gold_aggregation                │
│       → data_quality_check             │
│  Daily schedule | 2 retries             │
└─────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│            VISUALIZATION                │
│  Tableau Dashboard                      │
│  • Revenue by Region (Bar)              │
│  • Monthly Revenue Trend (Line)         │
│  • Profit by Category (Bar)             │
│  • Top Products by Profit (Bar)         │
│  • Profit Margin by Sub Category        │
└─────────────────────────────────────────┘
```

---

##  Tech Stack

| Layer | Technology |
|-------|------------|
| Data Processing | Apache Spark, PySpark, Spark SQL |
| Storage Format | Delta Lake |
| Platform | Databricks (Serverless, Unity Catalog) |
| Orchestration | Apache Airflow 2.8 (Docker) |
| Visualization | Tableau |
| Language | Python 3.12, SQL |
| Architecture Pattern | Medallion (Bronze / Silver / Gold) |

---

##  Project Structure

```
retail-medallion-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb        # Raw ingestion with metadata
│   ├── 02_silver_transformation.ipynb   # Cleaning & feature engineering
│   └── 03_gold_aggregations.ipynb       # Analytics aggregations
│
├── dags/
│   └── retail_pipeline_dag.py           # Airflow DAG definition
│
├── dashboard/
│   └── retail_analytics_dashboard   # Tableau dashboard 
│
├── data/
│   └── sample_orders.csv                # Sample dataset (100 rows)
│
└── README.md
```

---

##  How to Run

### Prerequisites
- Databricks account (Community Edition or Trial)
- Docker Desktop installed
- Python 3.8+

### Step 1 — Set up Databricks
1. Upload `data/orders.csv` to Databricks Volume
2. Run notebooks in order: 01 → 02 → 03

### Step 2 — Set up Airflow locally
```
# Clone the repo
git clone https://github.com/Varshithaa-K/retail-medallion-pipeline.git
cd retail-medallion-pipeline

# Create Airflow folders
mkdir logs plugins

# Start Airflow
docker compose up -d

# Open UI
# Go to http://localhost:8080
# Username: airflow | Password: airflow
```

### Step 3 — Trigger the pipeline
1. Open Airflow UI at `http://localhost:8080`
2. Enable `retail_medallion_pipeline` DAG
3. Click run to trigger manually

---

##  Key Results

| Metric                  | Value                    |
|-------------------------|--------------------------|
| Total Records Processed | 9,994                    |
| Pipeline Layers         | 3 (Bronze, Silver, Gold) |
| Gold Tables Created     | 2                        |
| Airflow DAG Tasks       | 4                        |
| Top Region by Revenue   | West ($699,859)          |
| Highest Profit Category | Technology ($76,433)     |
| Best Profit Margin      | Technology (8.28%)       |


### Step 4 — Set up Tableau Dashboard


#### Connect via CSV (Tableau Public)
1. Export Gold tables from Databricks notebook
2. Download CSVs from Databricks → Catalog → Volumes
3. Open **Tableau Public** app (free)
4. Connect → **Text File** → select CSV
5. Build dashboard and publish to Tableau Public

#### Dashboard Sheets
| Sheet                         | Chart Type     | Data Source          |
|-------------------------------|----------------|----------------------|
| Revenue by Region             | Bar Chart      | gold_sales_by_region |
| Monthly Revenue Trend         | Line Chart     | gold_sales_by_region |
| Profit by Category            | Bar Chart      | gold_sales_by_region |
| Top Products by Profit        | Horizontal Bar | gold_top_products    |
| Profit Margin by Sub Category | Horizontal Bar | gold_top_products    |

---


##  Key Features

**Medallion Architecture**
Clean separation of raw, cleaned, and aggregated data across three Delta Lake layers.

**Delta Lake**
ACID transactions, schema enforcement, and time travel on all pipeline layers.

**Feature Engineering**
Derived business metrics including profit, sale price, discount amount, and profit margin percentage directly in the pipeline.

**Deduplication**
Window function based deduplication using ingestion timestamp to handle reprocessing scenarios.

**Data Lineage**
Every Bronze record tagged with `source_file`, `pipeline_name`, and `ingestion_timestamp` for full traceability.

**Orchestration**
Apache Airflow DAG with dependency chaining, 2-retry logic, and daily scheduling at 6AM.

**Interactive Dashboard**
Tableau dashboard with regional revenue analysis, monthly trends, category profitability, and product rankings.

---

## Author

**Varshitha Konidena**
- 🔗 [LinkedIn](https://www.linkedin.com/in/varshitha-k-220b6b24b)

---

##  Dataset

Source: [Retail Orders Dataset — Kaggle](https://www.kaggle.com/datasets/ankitbansal06/retail-orders)

Records: 9,994 retail orders across 4 regions, 3 categories, and 17 sub-categories.

---
