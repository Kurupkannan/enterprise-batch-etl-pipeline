# Enterprise Transaction ETL Pipeline

An end-to-end batch ETL pipeline orchestrating data movement from raw ingestion to analytical modeled tables using **Apache Airflow** and **PySpark**. This project follows the **Medallion Architecture** (Bronze → Silver → Gold) to process transaction data.

## 🚀 Pipeline Architecture

1.  **Ingestion (Bronze Layer)**:
    -   Reads raw transaction files (`.xlsx` / `.csv`).
    -   Validates schema against `config/schema.json`.
    -   Stores raw data in CSV format, partitioned by ingestion date.

2.  **Transformation (Silver Layer)**:
    -   Uses **Apache Spark** to clean, dedup, and normalize data.
    -   Transforms raw records into structure Parquet files.

3.  **Warehousing (Gold Layer)**:
    -   **Load Dimensions**: Extracts and updates dimension tables (Customers, Products).
    -   **Load Facts**: Aggregates transaction data into Fact Sales tables for analytics.

4.  **Data Quality Checks**:
    -   Runs validation scripts to ensure data integrity and completeness after the run.

---

## 🛠 Prerequisites

Ensure you have the following installed in your environment (WSL2 / Linux / macOS recommended):

-   **Python 3.8+**
-   **Java 8 or 11** (Required for Apache Spark)
-   **Apache Spark 3.x**
-   **Apache Airflow**

---

## 📦 Installation & Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd enterprise-transaction-etl
```

### 2. Set Up Virtual Environment
Create and activate a Python virtual environment to manage dependencies:
```bash
# Create venv
python -m venv .venv

# Activate venv (Linux/WSL)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Initialize Airflow
Set up the local Airflow environment within the project directory:
```bash
# Set AIRFLOW_HOME to the current directory's airflow folder
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize the Airflow database
airflow db init

# Create an Admin user (Password will be prompted)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

## ⚙️ Configuration

### Update Project Paths
Open `airflow/dags/transaction_etl_dag.py` and ensure the `PROJECT_ROOT` variable points to your absolute project path in WSL/Linux format.

```python
# airflow/dags/transaction_etl_dag.py
PROJECT_ROOT = "/mnt/c/Users/YourName/path/to/enterprise-transaction-etl"
```

### Data Placement
-   Place raw input files in `data/sample/`.
-   The pipeline expects `config/schema.json` to be present for validation.

---

## ▶️ Running the Pipeline

You need to run the Airflow **Scheduler** and **Webserver** to execute the pipeline.

### Terminal 1: Scheduler
 Continuously checks for tasks to run.
```bash
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

### Terminal 2: Webserver
Hosts the UI to manage and monitor DAGs.
```bash
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver -p 8081
```

### Trigger the DAG
1.  Open your browser and navigate to `http://localhost:8081`.
2.  Login with the credentials created earlier (default: `admin`).
3.  Locate **`enterprise_transaction_etl`** in the DAGs list.
4.  Toggle the **Unpause** switch (ON).
5.  Click the **Trigger DAG** (Play button) to start a manual run.

---

## 📂 Project Structure

```text
enterprise-transaction-etl/
├── airflow/               # Airflow home (config, logs, DB)
│   ├── dags/              # DAG definitions
│   └── airflow.cfg        # Airflow configuration
├── config/                # Schema and configuration files
├── data/                  # Data storage (Raw, Bronze, etc.)
├── ingestion/             # Python scripts for data ingestion
├── transformations/       # PySpark scripts for data transformation
├── warehouse/             # Scripts for loading Dimensions and Facts
├── quality/               # Data Quality check scripts
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```
