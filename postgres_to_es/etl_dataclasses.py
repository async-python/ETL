import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union


@dataclass()
class PgRowsCount:
    count: int


@dataclass
class PgFilmID:
    id: uuid.UUID
    updated_at: datetime


@dataclass
class PgFilmWork:
    id: uuid.UUID
    title: str
    description: str
    type: str
    genres: List[str] = None
    rating: float = None
    creation_date: Union[str, datetime] = None
    certificate: str = None
    age_limit: str = None
    file_path: str = None
    directors: List[str] = None
    actors: List[str] = None
    writers: List[str] = None

    def __post_init__(self):
        self.directors = self.clear_spaces(self.directors)
        self.actors = self.clear_spaces(self.actors)
        self.writers = self.clear_spaces(self.writers)
        self.creation_date = (self.creation_date.isoformat() if
                              self.creation_date is not None else None)

    def clear_spaces(self, rows):
        """
        Удаление лишних пробелов в результате объединения полей
        имени в строку полного имени.
        """
        return [
            row.strip(' ').replace('  ', ' ') for row in rows
        ] if rows else None
