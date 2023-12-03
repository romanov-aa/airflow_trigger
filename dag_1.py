from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime
from airflow.decorators import task


with DAG(dag_id="dag_1", start_date=datetime(2022, 1, 1), schedule_interval="0 0 * * *", catchup=False) as dag:
   
    @task()
    def check_tables_exist():       
        postgre_con = BaseHook.get_connection('Test_conn_pg')
        postgre_con_host = postgre_con.host
        postgre_con_user = postgre_con.login
        postgre_con_pass = postgre_con.password
        postgre_con_port = postgre_con.port

        connection_str = f"postgresql+psycopg2://{postgre_con_user}:{postgre_con_pass}@{postgre_con_host}:{ postgre_con_port}/postgres"
        engine = create_engine(connection_str)

        with engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            result = connection.execute(query)
            tables = [row[0] for row in result.fetchall()]

        if 'titanic' in tables and 'laptops' in tables and 'wld' in tables:
            print('nice')
        else:
            print(int(query))
            

    check_tables_exist()
        
