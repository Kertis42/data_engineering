from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from function.dshop_function import s_function
from function.config import dshop_config

tables = dshop_config().get('tables')

default_args = {
    'owner': 'Airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    'Dshop',
    description='Dshop_uploud_tables_to_silver',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 24,2,30),
    default_args=default_args
)

def etl(value):
    return PythonOperator(
        task_id=f'{value}_etl',
        python_callable=s_function,
        provide_context=True,
        op_kwargs={"value": value},
        dag=dag
    )

dummy1 = DummyOperator(task_id='dummy1', dag=dag)
dummy2 = DummyOperator(task_id='dummy2', dag=dag)

for tables in tables:
    dummy1 >> etl(tables) >> dummy2
