from truflation.data.task import Task
from truflation.data.connector import connector_factory, cache_
from truflation.data.source_details import SourceDetails
from typing import Callable, Dict
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class GeneralLoader:
    """
    The GeneralLoader class is responsible for reading data from various sources,
    transforming it if necessary, and storing it in a cache for later use. It
    uses the "connector_factory" to create reader and writer objects for
    different types of data sources and destinations.

    Parameters
    ----------
    None

    Attributes
    ----------
    writer: Connector
        A writer object that is used to write data to the cache.

    Methods
    -------
    run(source_details: SourceDetails, key: str) -> None:
        Reads data from the specified source, transforms certain columns to
        datetime format if they exist, and writes the data to cache.

    transform(transformer: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        Transforms the data stored in the cache using the provided
        transformer function.

    clear() -> None:
        Clears the data stored in the cache.

    replace_cache(cache: dict) -> None:
        Replaces the data in the cache with a new set of data.

    cache() -> dict:
        Returns the data currently stored in the cache.
    """
    def __init__(self):
        # self.writer = ConnectorCache("cache") # this was replaced because of suggetsions that .cache didn't exist (none possibility)
        self.writer = cache_.connector()

    def run(self, source_details: SourceDetails, key: str):
        s_type = source_details.source_type
        source = source_details.source
        # source_url = f'{source_details.source_type}:{source_details.source}'
        if s_type != "override":
            reader = connector_factory(s_type) \
                if isinstance(s_type, str) \
                else s_type
        else:
            reader = source_details.connector
        logger.debug(f'reading {source}')


        print(f'reading all...')
        print(f'source: {source}')
        # todo -- parser should go here, sending it into reading
        df = reader.read_all(source, *source_details.args,  **source_details.kwargs)

        # Parse # todo -- adjust and consider moving to connector.read_all
        if source_details.parser is not None:
            df = source_details.parser(df)
        #     Transform
        if source_details.transformer:
            df = source_details.transformer(df, **source_details.transformer_kwargs)

        if df is None:
            return
        if 'date' in df:
            df['date'] = pd.to_datetime(df['date'])  # make sure the 'date' column is in datetime format
        if 'createdAt' in df:
            df['createdAt'] = pd.to_datetime(df['createdAt'])
        self.writer.write_all(df, key=key)

    def transform(self, transformer: Callable[[Dict], Dict]):
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

# todo -- create write function that we can write to anything
