from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union
from uuid import UUID

OBJ_ID = Union[str, UUID]
OBJ_NAME = Union[str, str]


@dataclass
class PgRowsCount:
    count: int


@dataclass
class PgObjID:
    id: UUID
    updated_at: datetime


@dataclass
class PgFilmWork:
    id: UUID
    title: str
    description: str
    type: str
    genres: List[Dict[OBJ_ID, OBJ_NAME]] = None
    rating: float = None
    creation_date: Union[str, datetime] = None
    certificate: str = None
    age_limit: str = None
    file_path: str = None
    directors: List[Dict[OBJ_ID, OBJ_NAME]] = None
    actors: List[Dict[OBJ_ID, OBJ_NAME]] = None
    writers: List[Dict[OBJ_ID, OBJ_NAME]] = None

    def __post_init__(self):
        self.directors = clear_spaces_list(self.directors)
        self.actors = clear_spaces_list(self.actors)
        self.writers = clear_spaces_list(self.writers)
        self.creation_date = format_datetime_iso(self.creation_date)


@dataclass
class PgGenre:
    id: UUID
    name: str
    description: str


@dataclass
class PgPerson:
    id: UUID
    full_name: str
    birth_date: Optional[Union[str, datetime]] = None
    role: Optional[List[str]] = None
    films: Optional[Union[str, list[str]]] = None

    def __post_init__(self):
        self.full_name = clear_spaces(self.full_name)
        self.birth_date = format_datetime_iso(self.birth_date)
        if type(self.films) == str:
            self.films = self.films.replace(
                '{', '').replace('}', '').split(',')


def clear_spaces(row) -> str:
    """
    Удаление лишних пробелов в результате объединения полей
    имени в строку полного имени.
    """
    return row.strip(' ').replace('  ', ' ')


def clear_spaces_list(array) -> Union[List[Dict[OBJ_ID, OBJ_NAME]], None]:
    """
    Удаление лишних пробелов в результате объединения полей
    имени в строку полного имени.
    """
    if array:
        for dictionary in array:
            dictionary['name'] = clear_spaces(dictionary['name'])
        return array
    return None


def format_datetime_iso(date) -> Union[datetime, None]:
    """Приведение даты в iso формат"""
    return date.isoformat() if date is not None else None
