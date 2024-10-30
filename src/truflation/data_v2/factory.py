import logging
from src.truflation.data_v2.types import SourceType, DestinationType
from truflation.data_v2.extractor.base import BaseExtractor
from truflation.data_v2.loader.base import BaseLoader
from truflation.data_v2.extractor.csv import CSVExtractor
from truflation.data_v2.loader.csv import CSVLoader

class ExtractorFactory:
    @staticmethod
    def create_extractor(source_type: SourceType) -> BaseExtractor:
        if source_type == SourceType.CSV:
            return CSVExtractor()
        else:
            logging.warning(f"No suitable extractor found for source type: {source_type}")
            return None

class LoaderFactory:
    @staticmethod
    def create_loader(destination_type: DestinationType) -> BaseLoader:
        if destination_type == DestinationType.CSV:
            return CSVLoader()
        else:
            logging.warning(f"No suitable loader found for destination type: {destination_type}")
            return None