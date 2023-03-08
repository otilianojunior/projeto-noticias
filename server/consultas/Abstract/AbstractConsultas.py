from abc import ABC
from newspaper import Article
from server.config.MongoDBConfig import MongoDBConfig


class AbstractConsultas(ABC):
    def __init__(self, url):
        self.url = url
        self.article = Article(url)
        self.collection = MongoDBConfig.get_client()['noticias-diarias-db']['noticias']

    def download_and_parse(self):
        self.article.download()
        self.article.parse()

    def get_title(self):
        return self.article.title

    def get_publish_date(self):
        return self.article.publish_date

    def get_authors(self):
        return self.article.authors

    def get_text(self):
        return self.article.text

    def get_images(self):
        return self.article.images

    def insertMany(self, documents):
        result = self.collection.insert_many(documents)
        return result.inserted_ids
