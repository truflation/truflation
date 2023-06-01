from typing import Callable
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails


class PipeLineDetails:
    def __init__(self,
                 name,
                 sources: list[SourceDetails],
                 exports: list[ExportDetails],
                 cron_schedule: dict = None,
                 pre_ingestion_function: Callable = None,
                 post_ingestion_function: Callable = None,
                 transformer: Callable = lambda x: x
                 ):
        self.name = name
        self.sources = sources
        self.exports = exports
        self.cron_schedule = cron_schedule if cron_schedule else {'hour': '1'}  # defaults to 1 am
        self.pre_ingestion_function = pre_ingestion_function
        self.post_ingestion_function = post_ingestion_function
        self.transformer = transformer
        self.first = 123
