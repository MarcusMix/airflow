from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import statistics as sts


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
    "pipeline_etl_simple", 
    default_args=default_args,
    description="My pipeline_etl_simple dag",
    schedule_interval="@daily", 
    catchup=False
)

def data_cleaner():
    dataset = pd.read_csv("/opt/airflow/data/churn.csv", sep=";")
    dataset.columns = ["id", "score", "uf", "gender", "age", "heritage", "balance", "products", "have_credit_card", "active", "wage", "exit"]
    
    # preencher valores ausenter salario com a mediana
    average_wage = sts.median(dataset["wage"])
    dataset["wage"].fillna(average_wage, inplace=True)
    
    # preencher generos ausentes com Masculino
    dataset["gender"].fillna("Masculino", inplace=True)
    
    dataset["gender"] = dataset["gender"].replace({"F": "Feminino", "Fem": "Feminino", "M": "Masculino"})
    
    # preencher idades menor que 0 e maior que 120 pela mediana
    median_age = sts.median(dataset["age"])

    dataset.loc[(dataset["age"] < 0) | (dataset["age"] > 120), "age"] = median_age
    
    # deletar id repetidos, deixar o primeiro
    dataset.drop_duplicates(subset="id", keep="first", inplace=True)
    
    # data atual
    data_atual = datetime.now()
    data_formatada = data_atual.strftime("%Y-%m-%d")
    
    # salvando arquivo limpo
    dataset.to_csv(f"/opt/airflow/data/churn_clean_{data_formatada}.csv", sep=";", index=False)
    
send_email = EmailOperator(task_id="send_email",
                           to="vini6789.98@gmail.com",
                           subject="Airflow Pipeline ETL running",
                           html_content=""" 
                                <h3>Pipeline ETL rodou!</h3>
                                <p>A DAG pipeline_etl_simple foi executada!</p>
                           """,
                           dag=my_dag,
                           trigger_rule="all_done")
    
simple_etl = PythonOperator(
    task_id="simple_etl", 
    python_callable=data_cleaner,
    dag=my_dag
)

simple_etl >> send_email