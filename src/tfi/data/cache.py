from tfi.data.reader import Reader
from tfi.data.writer import Writer

class ReaderCache(Reader):
    def __init__(self, cache, default_key = None):
        self.default_key = default_key
        self.cache = cache

    def read_all(self, *args, **kwargs):
        key = kwargs.get('key', self.default_key)
        if key is not None:
            return self.cache.get(key)

class WriterCache(Writer):
    def __init__(self, cache, default_key = None):
        self.default_key = default_key
        self.cache = cache

    def write_all(self, value, *args, **kwargs):
        key = kwargs.get('key', self.default_key)
        self.cache.set(key, value)

class Cache:
    def __init__(self):
        self.cache_data = {}

    def set(self, key, value):
        self.cache_data[key] = value

    def get(self, key):
        return self.cache_data[key]

    def reader(self, default_key = None):
        return ReaderCache(self, default_key)

    def writer(self, default_key = None):
        return WriterCache(self, default_key)

    

