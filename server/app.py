from fastapi import FastAPI
from app.config import settings


# CONTROLLER
from server.controller.NoticiasController import noticias


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1}/openapi.json"
)

# ROTAS
app.include_router(noticias, tags=['Notícias'])


@app.on_event("startup")
async def app_init():
    print('started')
