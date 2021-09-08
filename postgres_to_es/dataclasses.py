import uuid
from datetime import datetime

from pydantic.dataclasses import dataclass


@dataclass
class PgFilmWork:
    id: uuid.UUID
    title: str
    description: str
    type: str
    created_at: datetime
    updated_at: datetime
    genres: list = None
    rating: float = None
    creation_date: datetime = None
    certificate: str = None
    age_limit: str = None
    file_path: str = None
    directors: list = None
    actors: list = None
    writers: list = None
