from airflow.operators.python import PythonOperator
from airflow import DAG
import requests


from datetime import datetime

default_args = {
    'owner': 'Otiliano Junior',
    'start_date': datetime(2023, 3, 11, 8, 30, 0)
}

dag = DAG('api_request', default_args=default_args, schedule_interval='@daily')

def api_request(url):
    url_api = 'http://0.0.0.0:8000/api_noticias/diariamente'
    payload = {'url': url}
    response = requests.post(url_api, json=payload)


for url in ['https://www.bbc.com/portuguese/topics/c404v027pd4t', 'https://www.bbc.com/portuguese/topics/cr50y580rjxt']:
    task_id = f'request_{url.replace("://", "_").replace("/", "_")}'
    task = PythonOperator(
        task_id=task_id,
        python_callable=api_request,
        op_kwargs={'url': url},
        dag=dag
    )

