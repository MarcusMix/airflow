from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

my_dag = DAG(
    "xcom_test", 
    description="My xcom_test dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 4),
    catchup=False
    )

def task_write(**kwargs):
    kwargs["ti"].xcom_push(key="valorxcom1", value=10)
    

task1 = PythonOperator(task_id="tsk1", python_callable=task_write, dag=my_dag)


def task_read(**kwargs): 
    valor = kwargs["ti"].xcom_pull(key="valorxcom1")
    print(f"Valor recuperado: {valor}")

task2 = PythonOperator(task_id="tsk2", python_callable=task_read, dag=my_dag)

task1 >> task2