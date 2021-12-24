from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from function.stock_function import stock
from function.stock_function import s_stock

dag = DAG(
    'Stock',
    description='Stock_data_from_api',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 24, 1,30)
)

default_args = {
    'owner': 'Airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

task_json = PythonOperator(
    task_id='Json_to_bronze',
    dag = dag,
    python_callable= stock,
    provide_context=True,
    op_kwargs={"f_date": '{{ ds }}'}
)

task_etl = PythonOperator(
    task_id='Stock_etl',
    dag = dag,
    python_callable= s_stock,
    provide_context=True,
    op_kwargs={"f_date": '{{ ds }}'}
)

task_json>> task_etl
