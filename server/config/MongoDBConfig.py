import os
from dotenv import load_dotenv
from pymongo import MongoClient


class MongoDBConfig:
    __instance = None

    @staticmethod
    def get_instance():
        if MongoDBConfig.__instance is None:
            MongoDBConfig()
        return MongoDBConfig.__instance

    @staticmethod
    def get_client():
        return MongoDBConfig.get_instance().client

    def __init__(self):
        if MongoDBConfig.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            MongoDBConfig.__instance = self

        load_dotenv()
        self.uri = os.environ.get('MONGO_CONNECTION_STRING')
        self.client = MongoClient(self.uri)
