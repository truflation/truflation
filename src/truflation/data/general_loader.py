from truflation.data.task import Task
from truflation.data.connector import connector_factory
from truflation.data.source_details import SourceDetails
from typing import Callable
import pandas as pd


class GeneralLoader:
    def __init__(self):
        self.writer = connector_factory("cache")

    def run(self, source_details: SourceDetails, key: str):
        s_type = source_details.source_type
        source = source_details.source
        # source_url = f'{source_details.source_type}:{source_details.source}'
        reader = connector_factory(s_type) \
            if isinstance(s_type, str) \
            else s_type
        df = reader.read_all(source)
        if 'date' in df:
            df['date'] = pd.to_datetime(df['date'])  # make sure the 'date' column is in datetime format
        if 'createdAt' in df:
            df['createdAt'] = pd.to_datetime(df['createdAt'])

        self.writer.write_all(df, key=key)

    def transform(self, transformer: Callable):
        """ transforms cache with transformer function   """
        self.writer.cache.cache_data.update(transformer(self.writer.cache.cache_data))

    def clear(self):
        """ Clears the cache"""
        self.writer.cache.cache_data = {}

    def replace_cache(self, cache: dict):
        """ Clears the cache"""
        self.writer.cache.cache_data = cache

    # todo -- write
    # Create a generalized writer that accepts a cache_key_string, an export type, and a output location, which will take the df from the string in cache and output it in the expected type to the output location

    @property
    def cache(self):
        return self.writer.cache.cache_data
