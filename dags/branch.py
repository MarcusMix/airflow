from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
import random

my_dag = DAG(
    "branch_test", 
    description="My branch_test dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 9),
    catchup=False
    )

def get_random_number():
    return random.randint(1, 10)


get_random_number_task = PythonOperator(
    task_id="get_random_number_task",
    python_callable=get_random_number,
    dag=my_dag
)
    
def evaluate_number(**context):
    number = context["task_instance"].xcom_pull(task_ids="get_random_number_task")
    if number % 2 == 0:
        return "negative_task"
    else:
        return "positive_task"
    

branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=evaluate_number,
    provide_context=True,
    dag=my_dag
)


negative_task = BashOperator(task_id="negative_task", bash_command="echo 'negative number'", dag=my_dag)
positive_task = BashOperator(task_id="positive_task", bash_command="echo 'positive number'", dag=my_dag)


get_random_number_task >> branch_task
branch_task >> positive_task
branch_task >> negative_task