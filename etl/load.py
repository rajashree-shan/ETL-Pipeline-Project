from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

def load_to_redshift(df):
    conn = BaseHook.get_connection('redshift_conn')
    url = f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
    engine = create_engine(url)
    df.to_sql('sales_analytics', engine, index=False, if_exists='append')
