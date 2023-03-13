import logging
import requests
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
    'BBC Brasil': 'https://www.bbc.com/portuguese/topics/cz74k717pw5t',
    'BBC Economia': 'https://www.bbc.com/portuguese/topics/cvjp2jr0k9rt',
    'BBC Tecnologia': 'https://www.bbc.com/portuguese/topics/c404v027pd4t',

    'UOL NOTICIAS': 'https://noticias.uol.com.br/',
    'UOL ECONOMIA': 'https://economia.uol.com.br/',
    'UOL Esporte': 'https://www.uol.com.br/esporte/',

    'Folha ultimas noticias': 'https://www1.folha.uol.com.br/ultimas-noticias/',
    'Folha Politica': 'https://www1.folha.uol.com.br/poder/',
    'Folha Mundo': 'https://www1.folha.uol.com.br/mundo/',
    'Folha Cotidiano': 'https://www1.folha.uol.com.br/cotidiano/',

    'ESTADÃO CULTURA': 'https://www.estadao.com.br/cultura/',
    'ESTADÃO BRASIL': 'https://www.estadao.com.br/brasil/',
    'ESTADÃO ECONOMIA':'https://www.estadao.com.br/economia/',
    'ESTADÃO Politica': 'https://www.estadao.com.br/politica/',

    'G1': 'https://g1.globo.com/'
}


def api_request(url):
    url_api = 'http://0.0.0.0:8000/api_noticias/diariamente'
    payload = {'url': url}
    try:
        response = requests.post(url_api, json=payload)
        response.raise_for_status()
        logging.info(f"Request to {url} successful. Response: {response.json()}")
    except RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        

def optimize_url_selection(sites):
    # Remove duplicate URLs from the "sites" dictionary
    unique_urls = set(sites.values())
    return {k: v for k, v in sites.items() if v in unique_urls}


sites = optimize_url_selection(sites)

session = FuturesSession(executor=ThreadPoolExecutor(max_workers=len(sites)))


for site_name, url in sites.items():
    task_id = f'request_{site_name.lower().replace(" ", "_")}'
    task = PythonOperator(
        task_id=task_id,
        python_callable=api_request,
        op_kwargs={'url': url},
        dag=dag
    )


