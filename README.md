# Taxi_Analytics

## 📋 Project Overview
A data pipeline for processing NYC taxi trip data from raw CSV files to analytical gold tables using DuckDB and dbt. The pipeline ingests taxi trip records, zone lookups, and related data to create a clean, queryable dataset for analysis.

## 🏗️ Architecture
Local CSV Files → DuckDB (Raw) → dbt (Silver Layer) → dbt (Gold Layer)
## 🛠️ Setup Instructions

### Prerequisites
- Python 3.8+
- DuckDB
- dbt-core
- Docker

### Instrctions
1. Clone the repositroy.
2. Create a virtual environment for python 3.11 and install dbt-core, duckdb and all the packages.
3. Load the files into dbt-project/taxi/data/raw/ folder.
4. Run the docker container to start kestra and create a flow based on the yml present in the repo and execute the flow.
5. Create the view in bronze layer.
6. Create the tables in silver layer.

### Next Steps
1.Implement a schedule in kestra to ingest the raw files and also to trigger the dbt models via dbt cli. 
2.Implement an incremental model to ingest the data into bronze, silver and gold layers.
3.Create generic tests to improve data quality in silver layer.
4.Create facts and dimensions in the gold layer.
