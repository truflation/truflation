"""
Connector
"""

import os
import json
from typing import Optional, Iterator, Any, List
from pathlib import Path
import pandas
import gspread as gs
import requests
from playwright.sync_api import sync_playwright

import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, MetaData

class Connector:
    """
    Base class for Import
    """

    def __init__(self, *args, **kwargs):
        pass

    def authenticate(self, token: str):
        pass

    def read_all(
            self,
            *args,
            **kwargs
    ) -> Any:
        """
        Read Source file and parse through parser

        return: DataPandas, the data, of which a dataframe can be accessed via x.df
        """

        data = None
        while True:
            b = self.read_chunk(b)
            if b is None:
                break
            data = b
        return data

    def read_chunk(
            self,
            outputb,
            *args,
            **kwargs
    ) -> Any:
        return None

    def write_all(
            self,
            data,
            *args, **kwargs
    ) -> None:
        for i in self.write_chunk(
                data
        ):
            pass

    def write_chunk(
            self,
            data,
            *args, **kwargs
    ) -> Iterator[Any]:
        raise NotImplementedError


class ConnectorCache(Connector):
    def __init__(self, cache, default_key=None):
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

    def connector(self, default_key=None):
        return ConnectorCache(self, default_key)


class ConnectorCsv(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_root = kwargs.get('path_root', os.getcwd())
        Path(self.path_root).mkdir(parents=True, exist_ok=True)

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        filename = os.path.join(self.path_root, args[0])
        if os.access(filename, os.R_OK):
            return pandas.read_csv(filename)
        else:
            return None

    def write_all(
            self,
            data,
            *args, **kwargs) -> None:
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        filename = os.path.join(self.path_root, filename)
        data.to_csv(filename)


class ConnectorJson(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_root = kwargs.get('path_root', os.getcwd())
        Path(self.path_root).mkdir(parents=True, exist_ok=True)

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        if isinstance(filename, str):
            with open(os.path.join(self.path_root, filename)) as fileh:
                obj = json.load(fileh)
                return obj
        else:
            return json.load(fileh)

    def write_all(
            self,
            data,
            *args, **kwargs) -> None:
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        if isinstance(filename, str):
            filename = os.path.join(self.path_root, filename)
            with open(filename, 'w') as fileh:
                fileh.write(json.dumps(data, default=str))
        else:
            if isinstance(data, str):
                print(data, file=filename)
            else:
                filename.write(json.dumps(data, default=str))


class ConnectorSql(Connector):
    def __init__(self, engine):
        super().__init__()
        self.engine = create_engine(engine)

    def read_all(
            self,
            *args, **kwargs) -> Any:
        try:
            df = pandas.read_sql(args[0], self.engine)
            return df
        except Exception as e:
            return None

    def write_all(
            self,
            data,
            *args,
            **kwargs
    ) -> None:
        table = kwargs.pop('key', kwargs.pop('table', None))
        if table is None and len(args) > 0:
            table = args[0]
        with self.engine.connect() as conn:
            data.to_sql(
                table,
                conn,
                **kwargs
            )

    def write_chunk(
            self,
            data,
            *args, **kwargs
    ) -> Iterator[Any]:
        self.write_all(data, *args, **kwargs)
        yield None

    def execute(self, statement_list: List[str], **line):
        with self.engine.connect() as conn:
            for statement in statement_list:
                conn.execute(text(statement), **line)

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

    def create_table(
            self,
            table_name: str,
            columns,
            **params):
        metadata = MetaData()
        print(columns)
        Table(table_name, metadata, *columns)
        metadata.create_all(self.engine, **params)


class ConnectorRest(Connector):
    def __init__(self, base_, **kwargs):
        super().__init__()
        self.base = base_
        self.playwright = kwargs.get('playwright', False)
        self.json = kwargs.get('json', True)
        self.page = None

    def read_all(
            self,
            url, *args, **kwargs) -> Any:
        if self.playwright:
            with sync_playwright() as p:
                browser_type = p.firefox
                browser = browser_type.launch()
                self.page = browser.new_page()
                response = self.page.goto(
                    url
                )
                return self.process_response(response)

        response = requests.get(os.path.join(url))
        return self.process_response(response)

    def process_response(self, response):
        if self.json:
            return self.process_json(response.json())
        else:
            return self.process_content(response.content)

    @staticmethod
    def process_content(content):
        return content

    @staticmethod
    def process_json(json_obj):
        return json_obj



class ConnectorGoogleSheets(Connector):
    def read_all(self, sheet_id, *args, **kwargs) -> Any:
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export'
        # todo -- @joseph please change this to passing in a kwarg of 'sheet_name' and documenting the convention
        # if len(args) > 1:
        #     kwargs['sheet_name'] = args[1]
        df = pandas.read_excel(url, **kwargs)
        df.columns.values[1] = "value"
        df.rename(columns={'Date': 'date'}, inplace=True)
        return df


cache_ = Cache()


def connector_factory(connector_type: str) -> Optional[Connector]:
    if connector_type.startswith('cache'):
        return cache_.connector()
    if connector_type.startswith('csv'):
        if connector_type.startswith('csv:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorCsv(path_root=path_root)
        else:
            return ConnectorCsv()
    if connector_type.startswith('gsheet'):
        return ConnectorGoogleSheets()
    if connector_type.startswith('json'):
        # if source_location:
        #     return ConnectorJson(path_root=source_location)
        return ConnectorJson()
    if connector_type.startswith('playwright+http'):
        # return ConnectorRest(source_location, playwright=True)
        return ConnectorRest(connector_type, playwright=True)
    if connector_type.startswith('rest+http'):
        return ConnectorRest(connector_type)
    if connector_type.startswith('http'):
        return ConnectorRest(connector_type, json=False)
    if connector_type.startswith('sqlite') or \
            connector_type.startswith('postgresql') or \
            connector_type.startswith('mysql') or \
            connector_type.startswith('mariadb') or \
            connector_type.startswith('oracle') or \
            connector_type.startswith('mssql') or \
            connector_type.startswith('sqlalchemy') or \
            connector_type.startswith('gsheets') or \
            connector_type.startswith('pybigquery'):
        return ConnectorSql(connector_type)
    return None

def get_database_handle(db_type='mariadb+pymysql'):
    DB_USER = os.environ.get('DB_USER', None)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
    DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.environ.get('DB_PORT', None))
    DB_NAME = os.environ.get('DB_NAME', None)

    return f'{db_type}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

