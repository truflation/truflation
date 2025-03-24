from dataclasses import dataclass
from typing import Any
from enum import Enum, auto


class SourceType(Enum):
    CSV = auto()
    JSON = auto()
    SQL = auto()


class DestinationType(Enum):
    CSV = auto()
    JSON = auto()
    SQL = auto()


@dataclass
class Source:
    type: SourceType
    file_path: str


@dataclass
class Destination:
    type: DestinationType
    file_path: str

@dataclass
class PipelineDetails:
    name: str
    pre_ingestion_function: callable
    post_ingestion_function: callable
    source: Source
    destination: Destination
    transformer: callable[[dict], dict]