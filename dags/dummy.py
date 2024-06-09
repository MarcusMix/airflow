from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

my_dag = DAG(
    "dummy", 
    description="My dummy dag",
    schedule_interval=None, 
    start_date=datetime(2024, 6, 9),
    catchup=False
    )

task1 = BashOperator(task_id="tsk1", bash_command="sleep 1", dag=my_dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 1", dag=my_dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 1", dag=my_dag)
task4 = BashOperator(task_id="tsk4", bash_command="sleep 1", dag=my_dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", dag=my_dag)

task_dummy = DummyOperator(task_id="task_dummy", dag=my_dag)

[task1, task2, task3]>> task_dummy >> [task4, task5]