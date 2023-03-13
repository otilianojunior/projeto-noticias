from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional


class NOTICIADTO(BaseModel):
    data_hora_insercao: datetime
    titulo: str
    data_publicacao: Optional[str]
    autores: Optional[List[str]]
    texto: str
    imagens: Optional[List[str]]

    class Config:
        allow_population_by_field_name = True
