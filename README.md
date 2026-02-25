Airbnb ELT Pipeline on GCP

A production-grade ELT (Extract, Load, Transform) pipeline built on Google Cloud Platform to analyze Airbnb listing performance across Greater Sydney. The system integrates Airbnb listings data with Australian Census demographics using Apache Airflow for orchestration, PostgreSQL for warehousing, and dbt for transformation and modeling. The architecture follows the Medallion design pattern (Bronze → Silver → Gold) and implements Slowly Changing Dimension Type 2 (SCD2) tracking for historical accuracy.

Features

End-to-end ELT pipeline using GCP services

Automated orchestration with Apache Airflow

Medallion architecture (Bronze, Silver, Gold layers)

SCD Type 2 implementation using dbt snapshots

Idempotent monthly ingestion with duplicate prevention

Star schema data warehouse design

Analytical marts for business reporting

Development and production environment separation

Data quality validation with dbt tests

Structured SQL-based analytical queries

Installation
Prerequisites

Python 3.9+

PostgreSQL 13+

dbt (dbt-core and dbt-postgres)

Apache Airflow 2.x

Git

1. Clone the Repository
git clone https://github.com/your-username/airbnb-elt-gcp-airflow-dbt.git
cd airbnb-elt-gcp-airflow-dbt
2. Set Up Python Environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows

pip install -r requirements.txt
3. Configure PostgreSQL

Create a database and required schemas:

CREATE DATABASE airbnb_dw;

Update your Airflow and dbt connection configurations to point to the database.

4. Install dbt Dependencies
cd dbt
dbt deps
5. Configure Airflow

Initialize Airflow:

export AIRFLOW_HOME=~/airflow
airflow db init

Copy the DAG file:

cp airflow/load_to_bronze_parallel_dbt_2.py $AIRFLOW_HOME/dags/

Start Airflow services:

airflow webserver --port 8080
airflow scheduler
Usage Examples
Run Bronze Ingestion via Airflow

Trigger the DAG from the Airflow UI or CLI:

airflow dags trigger load_to_bronze_parallel_dbt_2
Run dbt Models (Development)
cd dbt
dbt run --select silver
dbt snapshot
dbt run --select gold
dbt test
Example Analytical Query
SELECT
    lga_name,
    AVG(avg_est_rev_per_active) AS avg_revenue
FROM analytics_gold.dm_listing_neighbourhood
GROUP BY lga_name
ORDER BY avg_revenue DESC;
Example dbt Snapshot (SCD Type 2)
{% snapshot snap_listing %}

{{
  config(
    target_schema='silver',
    unique_key='listing_id',
    strategy='timestamp',
    updated_at='scraped_date'
  )
}}

SELECT * FROM {{ ref('stg_listing') }}

{% endsnapshot %}
Project Structure
airbnb-elt-gcp-airflow-dbt/
│
├── airflow/
│   └── load_to_bronze_parallel_dbt_2.py
│
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── snapshots/
│   ├── dbt_project.yml
│   └── schema.yml
│
├── sql/
│   ├── bronze_schema_setup.sql
│   └── analysis_queries.sql
│
├── architecture/
│   ├── airflow_pipeline_diagram.png
│   └── dbt_lineage.png
│
└── docs/
    └── full_project_report.pdf
