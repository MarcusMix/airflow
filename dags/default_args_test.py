from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past' : False,
    'start_date' : datetime(2023, 6, 8),
    'email' : 'tradedomarcus@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(seconds=10)
}

my_dag = DAG(
    "default_args_test", 
    description="My default_args_test dag",
    default_args=default_args,
    schedule_interval='@hourly', 
    start_date=datetime(2024, 6, 8),
    catchup=False,
    default_view='graph',
    tags=['pipeline', 'test', 'tag', 'data']
)
    

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=my_dag, retries=3)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=my_dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=my_dag)

task1 >> task2 >> task3  