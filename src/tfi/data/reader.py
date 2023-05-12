"""
Reader
"""

# Constarints library
# https://tdda.readthedocs.io/en/latest/constraints.html

import os
from typing import Optional
import pandas
import requests

from sqlalchemy import create_engine
from data import Data, DataPandas, DataJson


class Reader:
    """
    Base class for Import
    """

    def __init__(self, parser=lambda x: x):
        pass
        # Function to parse or transform data received form API to wanted format
        self.parser: object = parser

    def authenticate(self, token):
        pass

    # todo -- @joseph this should return a standard format. I will assume a pandas dataframe
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


class ReaderSpecializedCsv(Reader):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def read_all(self) -> Optional[Data]:
        df = self.parser(pandas.read_csv(self.path))
        # df -->              <class 'pandas.core.frame.DataFrame'>
        # DataPandas(df) -->  <class 'data.DataPandas'>
        # return DataPandas(df)
        return df



class ReaderCsv(Reader):
    def __init__(self):
        super().__init__()

    def read_all(
            self, test,
            *args, **kwargs) -> Optional[Data]:
        print(test)
        print(args)
        print(kwargs)
        df = self.parser(pandas.read_csv(args[0]))
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


class ReaderRest(Reader):
    def __init__(self, base_):
        super().__init__()
        self.base = base_

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        response = requests.get(
            os.path.join(
                self.base,
                args[0]
            )
        )
        return DataJson(response.json())
