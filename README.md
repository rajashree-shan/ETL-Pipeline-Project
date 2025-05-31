# Airflow ETL: PostgreSQL to Redshift

## What It Does
- Extracts data from local PostgreSQL `sales` table
- Transforms it using Pandas
- Loads cleaned data into Amazon Redshift
- Sends a Slack notification upon completion

## Airflow Connections Needed
1. `postgres_conn` - PostgreSQL source
2. `redshift_conn` - Redshift target
3. `slack_webhook` - Slack incoming webhook

## Schedule
- Runs daily via Airflow DAG
