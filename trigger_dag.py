from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor



with DAG(dag_id="trigger_dag", start_date=datetime(2022, 1, 1), schedule_interval="0 0 * * *", catchup=False) as dag:

    wait_for_check_tables = ExternalTaskSensor(
        task_id='wait_for_pg_insert_dag',
        external_dag_id='dag_1',
        timeout=36000,
        mode='reschedule'
    )

    trigger_dag_2_task = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='dag_2', 
    )

    trigger_dag_3_task = TriggerDagRunOperator( 
        task_id='trigger_dag_3',
        trigger_dag_id='dag_3', 
    )

    trigger_dag_4_task = TriggerDagRunOperator(
        task_id='trigger_dag_4',
        trigger_dag_id='dag_4', 
    )

    wait_for_check_tables >> [trigger_dag_2_task, trigger_dag_3_task, trigger_dag_4_task]