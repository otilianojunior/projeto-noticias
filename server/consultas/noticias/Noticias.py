import requests
import datetime
from newspaper import Article
from bs4 import BeautifulSoup
from server.consultas.Abstract.AbstractConsultas import AbstractConsultas


class NoticiasConsulta(AbstractConsultas):
    def __init__(self, url):
        super().__init__(url)
        self.url = url

    class NoticiasConsulta(AbstractConsultas):
        def __init__(self, url):
            super().__init__(url)
            self.url = url

    def noticias(self):
        try:
            links = self.get_links()

            for link in links:
                if 'http' in link:
                    try:
                        noticia = self.get_info_noticia(link)
                        yield noticia
                    except:
                        pass

        except Exception as ex:
            print(ex)
            raise Exception

    def get_links(self):
        try:
            response = requests.get(self.url)
            soup = BeautifulSoup(response.content, 'html.parser')
            links = [a['href'] for a in soup.find_all('a', href=True)]
            return links
        except Exception as ex:
            print(ex)
            raise Exception

    def get_info_noticia(self, link):
        try:
            article = Article(link)
            article.download()
            article.parse()

            titulo = article.title
            data_publicacao = article.publish_date
            autores = article.authors
            texto = article.text

            noticia = {
                'data_hora_insercao': str(datetime.datetime.now()),
                'titulo': titulo,
                'data_publicacao': str(data_publicacao),
                'autores': autores,
                'texto': texto
            }

            return noticia
        except Exception as ex:
            print(ex)
            raise Exception
