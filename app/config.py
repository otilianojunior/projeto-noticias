from pydantic import BaseSettings


class Settings(BaseSettings):
    API_V1: str = "/api_noticias/"
    PROJECT_NAME: str = "Api de Notícias"


settings = Settings()
