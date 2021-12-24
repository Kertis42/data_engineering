from datetime import datetime
from airflow import DAG
from function.dshop_operator import DshopCsvOperator
from airflow.operators.dummy_operator import DummyOperator
from function.config import dshop_config

tables = dshop_config().get('tables')


def dowload(value):
    return DshopCsvOperator(
        dag=dag,
        task_id=f'{value}_to_bronze',
        postgres_conn_id='postgres_dshop',
        table_name=value,
        sql=f"(SELECT * FROM {value})"
    )

default_args = {
    'owner': 'Airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    'Dshop',
    description='Dshop_uploud_tables_to_bronze',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 24,2,0),
    default_args=default_args
)

dummy1 = DummyOperator(task_id='dummy1', dag=dag)
dummy2 = DummyOperator(task_id='dummy2', dag=dag)

for tables in tables:
    dummy1 >> dowload(tables) >> dummy2
