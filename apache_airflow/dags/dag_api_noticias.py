import requests
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def start_api():
    # Execute o script que inicia a API aqui
    # Exemplo: python /caminho/para/script/start_api.py
    pass


def requisicao_api_noticias():
    # Execute a requisição da API aqui
    response = requests.get('https://www.bbc.com/portuguese/topics/c404v027pd4t')

    # Exiba o conteúdo da resposta da API
    print(response.content)


default_args = {
    'owner': 'Otiliano Junior',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_api_noticias',
    default_args=default_args,
    description='DAG que executa a API uma vez por dia',
    schedule_interval=timedelta(days=1),
)

start_operator = BashOperator(
    task_id='start_api',
    bash_command='python /caminho/para/script/start_api.py',
    dag=dag
)

my_operator = PythonOperator(
    task_id='my_task',
    python_callable=requisicao_api_noticias,
    dag=dag,
)

start_operator >> my_operator
