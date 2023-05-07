"""
Reader
"""

import csv
import pandas
from typing import Optional
from sqlalchemy import create_engine
from tfi.data.data import Data, DataPandas

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
    ) -> Optional[Data]:
        data = None
        while True:
            b: Optional[Data] = self.read_chunk(b)
            if b is None:
                break
            data = b
        return data

    def read_chunk(
            self,
            outputb: Optional[Data],
            *args,
            **kwargs
    ) -> Optional[Data]:
        return None

class ReaderCSV(Reader):
    def __init__(self):
        super().__init__()

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        df = pandas.read_csv(args[0])
        return DataPandas(df)

class ReaderSql(Reader):
    def __init__(self, engine):
        super().__init__()
        self.engine = create_engine(engine)

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        df = pandas.read_sql(args[0], self.engine)
        return DataPandas(df)

class ReaderRestJson(Reader):
    def __init__(self, base_):
        super().__init__()
        self.base = base_
    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        pass
