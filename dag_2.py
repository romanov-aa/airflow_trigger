from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


def load_titanic_data_to_postgres():
    try:
        diabetes_data = pd.read_csv('/opt/airflow/dags/data/titanic.csv')

        postgres_conn = BaseHook.get_connection('Test_conn_pg')
        connection_str = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/postgres"
        engine = create_engine(connection_str)

        diabetes_data.to_sql('titanic', con=engine, index=False, if_exists='replace')

        print('Датасет записан в titanic')
    except Exception as e:
        print(f"Error: {e}")



with DAG(dag_id="dag_2", start_date=datetime(2022, 1, 1), schedule_interval="0 0 * * *", catchup=False) as dag:
    load_titanic_data_task = PythonOperator(
        task_id='load_titanic_data',
        python_callable=load_titanic_data_to_postgres
    )

    load_titanic_data_task
