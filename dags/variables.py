from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

my_dag = DAG(
    "variable", 
    description="My variable dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 9),
    catchup=False
    )

def print_variable(**context):
    my_key = Variable.get("key_test")
    print(f"Minha variable key is: {my_key}")

task1 = PythonOperator(task_id="tsk1", python_callable=print_variable, dag=my_dag)

task1