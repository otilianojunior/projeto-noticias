import os
import pymongo
import logging
import requests
import json
import bson.json_util
import collections
from airflow import DAG
from datetime import datetime
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor
from airflow.operators.python import PythonOperator
from requests_futures.sessions import FuturesSession

default_args = {
    'owner': 'Otiliano Junior',
    'start_date': datetime(2023, 3, 11, 8, 30, 0)
}

dag = DAG('api_request', default_args=default_args, schedule_interval='@daily')

sites = {
    'BBC Brasil': 'https://www.bbc.com/portuguese/topics/cz74k717pw5t'
}
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')


def get_mongo_connection():
    return pymongo.MongoClient(MONGO_CONNECTION_STRING)


def api_request(url):
    url_api = 'http://0.0.0.0:8000/api_noticias/diariamente'
    payload = {'url': url}
    try:
        response = requests.post(url_api, json=payload)
        response.raise_for_status()
        data = response.json()
        # trate os dados aqui, se necessário
        logging.info(f"Request to {url} successful. Response: {data}")
        return json.dumps(data)
    except RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        return None


def save_data_mongo(**context):
    data_str = context['ti'].xcom_pull(task_ids='api_request_task')
    if not data_str:
        logging.error('Não há dados para serem salvos no MongoDB.')
        return
    try:
        data = json.loads(data_str, object_hook=bson.json_util.object_hook)
        if not isinstance(data, dict):
            data = dict(data)
        client = get_mongo_connection()
        db = client['noticias-diarias-db']
        collection = db['noticias']
        result = collection.insert_many([data])
        logging.info('Dados salvos no MongoDB com sucesso. IDs: {}'.format(
            result.inserted_ids))
    except Exception as e:
        logging.error(
            'Erro ao salvar dados no MongoDB. Mensagem: {}'.format(str(e)))


def optimize_url_selection(sites):
    # Remove duplicate URLs from the "sites" dictionary
    unique_urls = set(sites.values())
    return {k: v for k, v in sites.items() if v in unique_urls}


sites = optimize_url_selection(sites)

session = FuturesSession(executor=ThreadPoolExecutor(max_workers=len(sites)))

for site_name, url in sites.items():
    task_id = f'request_{site_name.lower().replace(" ", "_")}'
    api_request_task = PythonOperator(
        task_id=task_id,
        python_callable=api_request,
        op_kwargs={'url': url},
        dag=dag
    )

    save_to_mongo_task = PythonOperator(
        task_id=f'save_to_mongo_{site_name.lower().replace(" ", "_")}',
        python_callable=save_data_mongo,
        provide_context=True,
        dag=dag,
    )

    api_request_task >> save_to_mongo_task
