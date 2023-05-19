from typing import Callable
from tfi.data.source_details import SourceDetails


class PipeLineDetails:
    def __init__(self,
                 name,
                 sources: list[SourceDetails],
                 exporter: SourceDetails,
                 pre_ingestion_function: Callable = None,
                 post_ingestion_function: Callable = None,
                 transformer: Callable = lambda x: x
                 ):
        self.name = name
        self.sources = sources
        self.exporter = exporter
        self.pre_ingestion_function = pre_ingestion_function
        self.post_ingestion_function = post_ingestion_function
        self.transformer = transformer
        self.first = 123
