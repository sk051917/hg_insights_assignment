# ETL Pipeline with Medallion Architecture

A production-ready ETL pipeline implementing the Medallion Architecture using Apache Airflow, Spark, Delta Lake, and Streamlit for data processing and visualization.

## Architecture Overview

This project implements a modern data lakehouse architecture with the following layers:

- **Source Layer**: Raw CSV data generated with synthetic customer, product, and order data
- **Raw**: Raw data ingested as-is from source systems
- **Structured**: Cleaned, deduplicated, and validated data

## Technologies Used

| Technology | Purpose |
|------------|---------|
| **Apache Airflow** | Workflow orchestration and scheduling (runs ETL pipeline hourly) |
| **Apache Spark** | Distributed data processing engine for large-scale transformations |
| **Delta Lake** | ACID-compliant storage layer with versioning and time travel capabilities |
| **Apache Derby** | Lightweight metastore for Hive metadata management |
| **Apache Hive** | SQL interface and metadata management for Spark tables |
| **Streamlit** | Interactive web dashboard for data visualization and monitoring |
| **PostgreSQL** | Backend database for Airflow metadata |
| **Docker & Docker Compose** | Containerization and service orchestration |

## Data Processing Flow

### Dataset Generation
The pipeline generates synthetic e-commerce data with realistic patterns. It mimics newly generated data by a source. For each run, it generates
- **Customers**: 1,000 customer records with demographics
- **Products**: 100 product records with categories and pricing
- **Orders**: 2,000 order transactions linking customers and products
- **Data Quality**: Intentionally includes 20% duplicates and 5% invalid records for testing data quality processes

### ETL Pipeline Stages

1. **Data Generation** (`generate_customer_data`)
   - Creates synthetic CSV files with controlled data quality issues
   - Located in `/opt/datasets/`

2. **Table Setup** (`setup_delta_tables`)
   - Initializes Delta Lake tables with proper schemas
   - Configures Hive metastore for table management

3. **Source to Raw (Bronze Layer)**
   - `customers_source_to_raw`: Ingests customer data
   - `orders_source_to_raw`: Ingests order transactions
   - `products_source_to_raw`: Ingests product catalog
   - No transformations applied - data stored as-is

4. **Raw to Structured (Silver Layer)**
   - `customers_raw_to_structured`: Deduplicates and validates customer records
   - `orders_raw_to_structured`: Validates orders and handles missing data
   - `products_raw_to_structured`: Cleans product data and standardizes formats
   - Data quality checks and cleansing applied

## Getting Started

### Prerequisites

1. **Install Docker Desktop**
   - Download from: https://www.docker.com/products/docker-desktop
   - Follow installation instructions for your operating system
   - Ensure Docker Desktop is running before proceeding

2. **Install Docker Compose** (if not included with Docker Desktop)
   - Docker Desktop for Mac and Windows includes Docker Compose
   - For Linux: Follow instructions at https://docs.docker.com/compose/install/

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/sk051917/hg_insights_assignment.git
   cd etl_pipeline
   ```

2. **Build Docker images**
   ```bash
   docker-compose build
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Wait for services to initialize** (approximately 1-2 minutes)
   - Check service status:
     ```bash
     docker-compose ps
     ```
   - View Airflow logs:
     ```bash
     docker-compose logs -f airflow-apiserver
     ```

## Accessing the Services

| Service | Port | Local URL | Remote URL (EC2/VM) |
|---------|------|-----------|---------------------|
| Airflow Web UI | 8080 | http://localhost:8080 | http://`<ip>`:8080 |
| Spark Master UI | 8081 | http://localhost:8081 | http://`<ip>`:8081 |
| Streamlit Dashboard | 8501 | http://localhost:8501 | http://`<ip>`:8501 |
| Spark Master | 7077 | spark://spark-master:7077 | (Internal only) |

**Default Airflow Credentials:**
- Username: `airflow`
- Password: `airflow`

> **Note**: When running on EC2 or other cloud VMs, replace `<ip>` with your instance's public IP address and ensure security groups allow inbound traffic on the required ports. Also please open ports on your firewall/security group to enable access.

## Pipeline Scheduling

The ETL pipeline runs **hourly** by default. To change the schedule:

1. Open `airflow/dags/ingestion_pipeline.py`
2. Locate the `schedule` parameter in the DAG definition:
   ```python
   schedule="@hourly",  # Change this line
   ```
3. Update with your desired schedule:
   - `"@daily"` - Run once per day at midnight
   - `"0 */6 * * *"` - Run every 6 hours
   - `"0 9 * * 1-5"` - Run at 9 AM on weekdays
   - `None` - Disable automatic scheduling (manual trigger only)

4. Restart Airflow to apply changes:
   ```bash
   docker-compose restart airflow-scheduler airflow-dag-processor
   ```

## Running the Pipeline

### Option 1: Trigger from Airflow UI
1. Navigate to http://localhost:8080 or http://<ip>:8080 (In case of vm)
2. Login with credentials (airflow/airflow)
3. Find the `etl_pipeline_medallion` DAG
4. Click the "Play" button to trigger a manual run

### Option 2: Trigger from Command Line
```bash
docker-compose exec airflow-apiserver airflow dags trigger etl_pipeline_medallion
```

### Option 3: Wait for Scheduled Run
The pipeline will automatically run every hour (at the top of the hour) based on the configured schedule.

## 📁 Project Structure

```
etl_pipeline/
├── airflow/
│   ├── config/
│   │   └── airflow.cfg          # Airflow configuration
│   ├── dags/
│   │   └── ingestion_pipeline.py # Main DAG definition
│   ├── logs/                     # Airflow task logs
│   └── plugins/                  # Custom Airflow plugins
│
├── dashboard/
│   └── app.py                    # Streamlit dashboard application
│
├── datasets/
│   └── .gitkeep                  # Generated data and Delta tables
│
├── jars/
│   └── derbyclient-10.14.2.0.jar # Derby JDBC driver
│
├── spark_code/
│   ├── adapters/
│   │   ├── data_reader.py        # Delta Lake reader utilities
│   │   ├── data_writer.py        # Delta Lake writer utilities
│   │   └── __init__.py
│   │
│   ├── config/
│   │   ├── hive-site.xml         # Hive metastore configuration
│   │   ├── schemas.py            # Table schema definitions
│   │   ├── setup_tables.py       # Delta table initialization
│   │   └── __init__.py
│   │
│   ├── execute/
│   │   ├── raw_to_structured/    # Silver layer transformations
│   │   │   ├── customers_raw_to_structured.py
│   │   │   ├── orders_raw_to_structured.py
│   │   │   ├── products_raw_to_structured.py
│   │   │   └── __init__.py
│   │   │
│   │   └── source_to_raw/        # Bronze layer ingestion
│   │       ├── customers_source_to_raw.py
│   │       ├── orders_source_to_raw.py
│   │       ├── products_source_to_raw.py
│   │       └── __init__.py
│   │
│   └── utils/
│       ├── logger.py             # Logging utilities
│       └── __init__.py
│
├── utils/
│   ├── generate_data.py          # Synthetic data generator
│   └── __init__.py
│
├── Dockerfile.airflow            # Airflow container definition
├── Dockerfile.spark              # Spark container definition
├── Dockerfile.streamlit          # Streamlit container definition
├── docker-compose.yml            # Multi-container orchestration
└── .gitignore                    # Git ignore rules
```

## Data Visualization with Streamlit
The project includes a Streamlit dashboard for real-time data visualization and monitoring of your ETL pipeline results.

### Accessing the Dashboard
Navigate to http://localhost:8501 (or http://<ip>:8501 for remote servers)

## Monitoring & Debugging

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master
docker-compose logs -f streamlit-dashboard
```

### Check Service Health
```bash
docker-compose ps
```

### Access Container Shell
```bash
# Airflow
docker-compose exec airflow-apiserver bash

# Spark Master
docker-compose exec spark-master bash

# Spark Worker
docker-compose exec spark-worker bash
```

### Inspect Delta Tables
```bash
# From Airflow container
docker-compose exec airflow-apiserver python

# Then in Python:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Inspector").getOrCreate()
spark.sql("SHOW TABLES").show()
```

## Stopping the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears all data)
docker-compose down -v
```