# Multi-Source FX & Macro Finance Pipeline (Hybrid ETL)

An end-to-end **ETL pipeline** for financial data integration, featuring a **Hybrid Cloud** architecture split between a local environment (**Ingestion**) and **Databricks** (**Processing**).

## Project Objective
To monitor the correlation between exchange rates (Forex) and macroeconomic assets (Gold, Oil, Equity Indices, and Bonds). The pipeline automates data extraction from APIs and transforms raw data into analytics-ready tables.

## System Architecture
The project adopts a hybrid approach to bypass external connectivity limitations often found in free cloud environments:
1. **Local Ingestion Tier (Python):** 
    - Extraction from **FastForex API** (Historical/Daily) and **yFinance** (Monthly Macro).
    - Data validation and metadata enrichment (`ingested_at`).
    - Automated upload to **Databricks Volumes** via the **Databricks SDK**.
2. **Cloud Processing Tier (Databricks & PySpark):** 
    - Implementation of the **Medallion Architecture** using Delta Tables.

## Repository Structure

```
.
├── local_ingestion/          # Data acquisition tier (Python)
│   ├── config/               # Data sources definition and API parameters (sources.yml)
│   ├── secrets/              # Credentials (git-ignored)
│   ├── historical_forex/     # Local storage for temporary CSVs (git-ignored)
│   ├── daily_forex/          # Local storage for temporary CSVs (git-ignored)
│   ├── monthly_macro_finance # Local storage for temporary Parquet files (git-ignored)
│   └── src/
│       ├── forex/            # Ingestion logic for FastForex API
│       ├── macro/            # Ingestion logic for yFinance
│       ├── to_databricks/    # Databricks SDK integration
│       └── utils/            # Config loader and path management
├── databricks_workspace/     # Data processing tier (PySpark)
│   ├── bronze/               # Raw data ingestion & metadata
│   ├── silver/               # Cleaning, typing, and deduplication
│   ├── gold/                 # Business logic & final joins
│   └── common/               # Shared logging utilities
├── .gitignore                # Rules for excluding sensitive and temporary files
├── README.md                 # Project documentation
└── requirements.txt          # Python dependencies for the local tier

```

## Technical Details

### 1. Ingestion & Validation (Local)
- **Data Sources:** FastForex (JSON) and yFinance (Parquet).
- **Metadata:** Every record includes an `ingested_at` timestamp. Data is staged locally in CSV/Parquet format before cloud upload.
- **Logging:** A centralized `logging` class tracks extraction progress, errors, and upload success.

### 2. Medallion Architecture (Databricks)
- **Bronze:** Loads data from Volumes to Delta Lake. Adds `processed_at`metadata.
- **Silver:** Data cleaning, schema enforcement (Decimal, Timestamp), normalization, and deduplication.
- **Gold:** Strategic joins between Forex and Macro assets to create correlation tables ready for BI tools.

## Setup and Installation

### Prerequisites
- Python 3.9+
- Databricks Free Edition Account
- Databricks Personal Access Token (PAT)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/toddina/etl-fx-macro-pipeline.git
   ```
2. Configure the environment:
    ```bash
    pip install -r requirements.txt
    ```
3. Configure credentials:
    - **Databricks SDK/CLI (`~/.databrickscfg`):** The project uses the default profile. Create or edit the file in your home directory:
    ```Ini, TOML
    [DEFAULT]
    host = https://dbc-<id-workspace>.cloud.databricks.com
    token = <your-personal-access-token>
    ```
    *(Note: Alternatively, you can set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables).*
    - **API Key (`local_ingestion/secrets/credentials.json`):** Create this file to store your data access keys. The required structure is:
    ```JSON
    {
        "fastforex_api_key": "your_api_key_here"
    }
    ```
## Future Improvements
- **Orchestration with Apache Airflow:** Automate the end-to-end workflow, managing dependencies between local ingestion and Databricks jobs.
- **Data Quality Testing:** Implement automated checks (e.g., Great Expectations) to ensure data integrity during the Silver layer transformation.
- **Interactive Dashboards:** Visualize insights using Databricks SQL or Power BI to monitor FX correlations and macro trends.

*Developed for educational purposes to demonstrate skills in Data Integration, PySpark, and Cloud Architectures.*