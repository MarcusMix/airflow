from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

my_dag = DAG(
    "second_dag", 
    description="My second dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 4),
    catchup=False
    )

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=my_dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=my_dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=my_dag)

task1 >> [task2, task3]  