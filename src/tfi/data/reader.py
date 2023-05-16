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
from tfi.data.data import Data, DataPandas, DataJson


class Reader:
    """
    Base class for Import
    """

    # returns DataPandas(df)
    def __init__(self, parser=lambda x: x):
        pass

    def authenticate(self, token):
        pass

    # todo -- @joseph decide on paramters, document. I am using 'source' and 'parser' in my specialized reader
    def read_all(
            self,
            *args,
            **kwargs
    ) -> Optional[Data]:
        """
        Read Source file and parse through parser

        return: DataPandas, the data, of which a dataframe can be accessed via x.df
        """

        data = None
        while True:
            b: Optional[Data] = self.read_chunk(b)
            if b is None:
                break
            data = b
        return data

    @staticmethod
    def read_chunk(
            outputb: Optional[Data],
            *args,
            **kwargs
    ) -> Optional[Data]:
        return None


# todo -- this will not be specialized and we will have to send in a link or path
# todo -- use this type:
#      DataPandas(df) -->  <class 'data.DataPandas'>
class ReaderSpecializedCsv(Reader):
    def __init__(self):
        super().__init__()

    def read_all(self, source, parser) -> Optional[Data]:
        df = parser(pandas.read_csv(source))
        # df -->              <class 'pandas.core.frame.DataFrame'>
        # DataPandas(df) -->  <class 'data.DataPandas'>
        data = DataPandas(df)
        data.df
        print(f' df type: {data.df}')
        return DataPandas(df)
        # return df



class ReaderCsv(Reader):
    def __init__(self):
        super().__init__()

    def read_all(
            self, test,
            *args, **kwargs) -> Optional[Data]:
        print(test)
        print(args)
        print(kwargs)
        df = pandas.read_csv(test)
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
