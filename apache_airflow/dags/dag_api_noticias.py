import os
import requests
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess


def start_api():
    command = "python /projeto-noticias/app/main.py"
    process = subprocess.Popen(command.split())
    return process.pid


def stop_api(pid):
    os.system(f"kill {pid}")


def requisicao_api_noticias(urls):
    for url in urls:
        response = None
        try:
            response = requests.get('http://0.0.0.0:8000/api_noticias/diariamente', params={'url': url}, timeout=180)
        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer requisição para a URL {url}: {str(e)}")

        if response is not None:
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
    description='DAG que executa a API a cada hora',
    schedule_interval=timedelta(hours=1),
)

start_operator = PythonOperator(
    task_id='start_api',
    python_callable=start_api,
    dag=dag,
)

urls = ['https://www.bbc.com/portuguese/topics/c404v027pd4t', 'https://www.bbc.com/portuguese/topics/cz74k717pw5t']

requisicao_api_noticias = PythonOperator(
    task_id='requisicao_api_noticias',
    python_callable=requisicao_api_noticias,
    op_kwargs={'urls': urls},
    dag=dag,
)

stop_operator = PythonOperator(
    task_id='stop_api',
    python_callable=stop_api,
    op_kwargs={'pid': '{{ ti.xcom_pull(task_ids="start_api") }}'},
    dag=dag,
)

start_operator >> requisicao_api_noticias >> stop_operator
