"""
Reader
"""

import csv
import pandas
from typing import Optional
from tfi.data.bundle import Bundle, BundlePandas
from sqlalchemy import create_engine

class Reader:
    """
    Base class for Import
    """
    def __init__(self):
        pass

    def read_all(
            self,
            *args,
            **kwargs
    ) -> Optional[Bundle]:
        bundle = None
        while True:
            b: Optional[Bundle] = self.read_chunk(b)
            if b is None:
                break
            bundle = b
        return bundle

    def read_chunk(
            self,
            outputb: Optional[Bundle],
            *args,
            **kwargs
    ) -> Optional[Bundle]:
        return None

class ReaderCSV(Reader):
    def __init__(self):
        super(Reader, self).__init__()

    def read_all(
            self,
            *args, **kwargs) -> Optional[Bundle]:
        df = pandas.read_csv(args[0])
        return BundlePandas(df)

class ReaderSql(Reader):
    def __init__(self, engine):
        super(Reader, self).__init__()
        self.engine = create_engine(engine)

    def read_all(
            self,
            *args, **kwargs) -> Optional[Bundle]:
        df = pandas.read_sql(args[0], self.engine)
        return BundlePandas(df)
