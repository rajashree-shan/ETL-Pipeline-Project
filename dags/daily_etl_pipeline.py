from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_from_postgres
from etl.transform import transform_data
from etl.load import load_to_redshift
from etl.notify import notify_slack

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'daily_postgres_to_redshift_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    def extract_task(**kwargs):
        df = extract_from_postgres()
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

    def transform_task(**kwargs):
        import pandas as pd
        raw_json = kwargs['ti'].xcom_pull(key='raw_data')
        df = pd.read_json(raw_json)
        clean_df = transform_data(df)
        kwargs['ti'].xcom_push(key='clean_data', value=clean_df.to_json())

    def load_task(**kwargs):
        import pandas as pd
        clean_json = kwargs['ti'].xcom_pull(key='clean_data')
        df = pd.read_json(clean_json)
        load_to_redshift(df)

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
        provide_context=True,
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=notify_slack,
    )

    extract >> transform >> load >> notify
