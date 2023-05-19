from tfi.data.task import Task
from tfi.data.connector import connector_factory
from tfi.data.source_details import SourceDetails
from typing import Callable

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
        d = reader.read_all(source)
        self.writer.write_all(d, key=key)

    def transform(self, transformer: Callable):
        self.writer.cache.cache_data.update(transformer(self.writer.cache.cache_data))

    # todo -- clear cache
    # todo -- replace cache

    @property
    def cache(self):
        return self.writer.cache.cache_data
