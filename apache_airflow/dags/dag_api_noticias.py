import os
import json
import logging
import requests
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv
from pymongo import MongoClient
from requests.exceptions import RequestException
from airflow.operators.python import PythonOperator, BranchPythonOperator



load_dotenv()

default_args = {
    'owner': 'Otiliano Junior',
    'start_date': datetime(2023, 3, 11, 8, 30, 0),
    'end_date': None,  # or a specific date
    'schedule_interval': timedelta(days=1)
}

dag = DAG('api_request', default_args=default_args)

sites = {
    'BBC Brasil': 'https://www.bbc.com/portuguese/topics/cz74k717pw5t',
    'ESTADÃO CULTURA': 'https://www.estadao.com.br/cultura/'
}

mongo_url = os.environ.get('MONGO_CONNECTION_STRING')
BASE_URL = 'http://0.0.0.0:8000/'
task_ids = {f'request_{site_name.lower().replace(" ", "_")}': site_name for site_name, url in sites.items()}


def api_request(url):
    url_api = BASE_URL + 'api_noticias/diariamente'
    payload = {'url': url}
    try:
        response = requests.post(url_api, json=payload)
        response.raise_for_status()
        logging.info(f"Request to {url} successful. Response: {response.json()}")
        return response.text
    except RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        return None



def connect_to_mongo(**kwargs):
    client = MongoClient(mongo_url)
    db = client['noticias-diarias-db']
    collection = db['noticias']
    noticias = collection.find()
    noticias_dict = [n.to_dict() for n in noticias]
    kwargs['ti'].xcom_push(key='noticias_dict', value=noticias_dict)
    kwargs['ti'].xcom_push(key='collection', value=collection) # adicionado
   


def save_data_to_mongo(**kwargs):
    noticias_dict = kwargs['ti'].xcom_pull(task_ids='connect_to_mongo', key='noticias_dict')
    collection = kwargs['ti'].xcom_pull(task_ids='connect_to_mongo', key='collection')
    documents = []
    for article in noticias_dict:
        document = {
            'title': article['titulo'],
            'url': article['url'],
            'data_publicacao': article.get('data_publicacao'),
            'autores': article.get('autores'),
            'texto': article['texto'],
            'imagens': article.get('imagens'),
            'data_hora_insercao': datetime.now()
        }
        documents.append(document)
    result = collection.insert_many(documents)
    logging.info(f"Inseridos {len(result.inserted_ids)} documentos no banco")



def fetch_and_save(url, mongo_url, **kwargs):
    response_text = api_request(url)
    if response_text is None:
        return
    response_data = json.loads(response_text)

    # Salva os dados no MongoDB
    collection = kwargs['ti'].xcom_pull(task_ids='connect_to_mongo', key='collection')
    save_data_to_mongo(collection=collection, **kwargs)

    # Define o resultado da tarefa como o nome do site
    return url.split('/')[-1]
 

# Cria a conexão com o MongoDB antes de executar as tasks de inserção de dados
connect_task = PythonOperator(
    task_id='connect_to_mongo',
    python_callable=connect_to_mongo,
    provide_context=True,  # Adicionado
    dag=dag
)


for site_name, url in sites.items():
    task_id = f'request_{site_name.lower().replace(" ", "_")}'
    fetch_task = PythonOperator(
        task_id=task_id,
        python_callable=fetch_and_save,
        op_kwargs={'url': url, 'mongo_url': mongo_url},
        provide_context=True,
        dag=dag
    )

    # Define a dependência da fetch_task em relação à connect_task
    fetch_task.set_upstream(connect_task)

    save_task = PythonOperator(
        task_id=f'save_to_mongo_{site_name.lower().replace(" ", "_")}',
        python_callable=save_data_to_mongo,
        op_kwargs={'data': '{{ ti.xcom_pull(task_ids=' + task_id + ') }}'},
        dag=dag
    )

    # Define a dependência da save_task em relação à fetch_task
    save_task.set_upstream(fetch_task)

save_task = PythonOperator(
    task_id='save_data_to_mongo',
    provide_context=True,
    python_callable=save_data_to_mongo,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="extract_data") }}',
               'collection': 'noticias'}
)

