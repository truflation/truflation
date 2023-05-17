"""
Connector
"""

import os
from typing import Optional, Iterator, Any
import pandas
import requests

import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from tfi.data.data import Data, DataPandas, DataJson, DataFormat



class Connector:
    """
    Base class for Import
    """

    def __init__(self, *args, **kwargs):
        pass

    def authenticate(self, token):
        pass

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

    def read_chunk(
            self,
            outputb: Optional[Data],
            *args,
            **kwargs
    ) -> Optional[Data]:
        return None

    def write_all(
            self,
            data: Data,
            *args, **kwargs
    ) -> None:
        for i in self.write_chunk(
                data
        ):
            pass

    def write_chunk(
            self,
            data: Data,
            *args, **kwargs
    ) -> Iterator[Any]:
        raise NotImplementedError

class ConnectorCache(Connector):
    def __init__(self, cache, default_key = None):
        super().__init__()
        self.default_key = default_key
        self.cache = cache

    def read_all(self, *args, **kwargs):
        key = kwargs.get('key', self.default_key)
        return self.cache.get(key) if key is not None else None

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

    def connector(self, default_key = None):
        return ConnectorCache(self, default_key)

class ConnectorCsv(Connector):
    def __init__(self):
        super().__init__()

    def read_all(
            self, *args, **kwargs
    ) -> Optional[Data]:
        df = pandas.read_csv(args[0])
        return DataPandas(df)

    def write_all(
            self,
            data: Data,
            *args, **kwargs) -> None:
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        data.get(DataFormat.PANDAS).to_csv(filename)


class ConnectorSql(Connector):
    def __init__(self, engine):
        super().__init__()
        self.engine = create_engine(engine)
        self.conn = self.engine.connect()

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        df = pandas.read_sql(args[0], self.engine)
        return DataPandas(df)

    def write_all(
            self,
            data: Data,
            *args,
            **kwargs
    ) -> None:
        table = kwargs['table']
        del kwargs['table']
        data.get(DataFormat.PANDAS).to_sql(
            table,
            self.engine,
            **kwargs
        )

    def write_chunk(
            self,
            data: Data,
            *args, **kwargs
    ) -> Iterator[Any]:
        self.write_all(data, *args, **kwargs)
        yield None

    def execute(self, statement, **line):
        self.conn.execute(statement, **line)

    def drop_table(
            self,
            table_name: str,
            ignore_fail: bool = True
    ):
        try:
            tbl = Table(
                table_name, MetaData(),
                autoload_with=self.engine
            )
        except sqlalchemy.exc.NoSuchTableError:
            if ignore_fail:
                return
            raise
        tbl.drop(self.engine, checkfirst=False)


class ConnectorRest(Connector):
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

cache_ = Cache()

def connector_factory(url: str) -> Optional[Connector]:
    if url == "cache":
        return cache_.connector()
    if url == "csv":
        return ConnectorCsv()
    return None
