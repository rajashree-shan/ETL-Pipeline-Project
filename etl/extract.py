import pandas as pd
from airflow.hooks.base import BaseHook
import psycopg2

def extract_from_postgres():
    conn = BaseHook.get_connection('postgres_conn')
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    df = pd.read_sql("SELECT * FROM sales", pg_conn)
    pg_conn.close()
    return df
