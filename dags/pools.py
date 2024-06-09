from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

my_dag = DAG(
    "pool", 
    description="My pool dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 9),
    catchup=False
    )

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=my_dag, pool="my_pool")
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=my_dag, pool="my_pool", priority_weight=2)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=my_dag, pool="my_pool")
task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", dag=my_dag, pool="my_pool", priority_weight=10)