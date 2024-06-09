from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past' : False,
    'start_date' : datetime(2023, 6, 9),
    'email' : 'vini6789.98@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(seconds=10)
}

my_dag = DAG(
    "email", 
    description="My email dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    default_view='graph',
    tags=['pipeline', 'test', 'email', 'data']
)
    

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=my_dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=my_dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=my_dag)
task4 = BashOperator(task_id="tsk4", bash_command="exit 1", dag=my_dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", dag=my_dag, trigger_rule="none_failed")
task6 = BashOperator(task_id="tsk6", bash_command="sleep 5", dag=my_dag, trigger_rule="none_failed")

send_email = EmailOperator(task_id="send_email",
                           to="vini6789.98@gmail.com",
                           subject="Airflow error",
                           html_content=""" 
                                <h3>Ocorreu um erro na DAG!</h3>
                                <p>A DAG task4 ocorreu um erro na execução!</p>
                           """,
                           dag=my_dag,
                           trigger_rule="one_failed")

[task1, task2] >> task3 >> task4 
task4 >> [send_email, task5, task6]