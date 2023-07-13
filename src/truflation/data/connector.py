"""
Connector
"""

import os
import json
from typing import Optional, Iterator, Any, List
from pathlib import Path
import pandas as pd
import gspread
from gspread_pandas import Spread, Client
import requests
from playwright.sync_api import sync_playwright
import logging

import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, MetaData

logger = logging.getLogger(__name__)

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

    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read data from a CSV file.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Optional[pd.DataFrame]: The data read from the CSV file as a pandas DataFrame, or None if the file is not accessible.
        """
        filename = os.path.join(self.path_root, args[0])
        if os.access(filename, os.R_OK):
            return pd.read_csv(filename)
        else:
            return None

    def write_all(self, data, *args, **kwargs) -> None:
        """
        Write data to a CSV file.

        Args:
            data: The data to be written.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None
        """
        filename = kwargs.get('key', None)
        if_exists = kwargs.get('if_exists', False)
        if filename is None and len(args) > 0:
            filename = args[0]
        filename = os.path.join(self.path_root, filename)
        if not os.path.exists(filename):
            return data.to_csv(
                filename
            )
        if if_exists == 'append':
            return data.to_csv(
                filename, mode='a', header=False
            )
        elif if_exists == 'replace':
            return data.to_csv(
                filename
            )
        else:
            raise ValueError

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
            return json.load(filename)

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


class ConnectorDirect(Connector):
    """ used for reading in DataFrames, dictionaries, json files directly as objects. Can not be written to???
    kwargs:
      data_type: str: the type of object that is passed in
    """
    def __init__(self, *args, **kwargs):
        super().__init__()

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        data_type = kwargs.get('data_type', None)
        data = kwargs.get('data', None)
        assert data_type is not None, "no data type specified"
        assert data is not None, "no data provided"
        assert type(data) is data_type, "data does not match data_type"

        return data

    def write_all(
            self,
            data,
            *args, **kwargs) -> None:
        raise NotImplementedError


class ConnectorSql(Connector):
    engines = {}

    def __init__(self, engine):
        super().__init__()
        self.engine = self.engines.get(engine)
        if self.engine is None:
            self.engine = \
                create_engine(engine)
            self.engines[engine] = self.engine
    # rollbacks are necessary to prevent timeouts
    # see https://stackoverflow.com/questions/58378708/sqlalchemy-cant-reconnect-until-invalid-transaction-is-rolled-back
    # with error Can't reconnect until invalid transaction is rolled back.  Please rollback() fully before proceeding (Background on this error at: https://sqlalche.me/e/20/8s2b)
    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        with self.engine.connect() as conn:
            try:
                return pd.read_sql(args[0], conn)
            except Exception as e:
                logger.debug(e)
                conn.rollback()
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
            try:
                data.to_sql(
                    table,
                    conn,
                    **kwargs
                )
            except:
                conn.rollback()
                raise

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
    def __init__(self, **kwargs):
        super().__init__()
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
    """
    A class for connecting and reading data from Google Sheets.

    Attributes:
        my_client: An instance of the Client class for Google Sheets API.
        default_key: The default key for the Google Sheets document.
        path_root: The root path for the Google Sheets document.
        client: The client instance for Google Sheets API.

    Methods:
        __init__: Initializes the ConnectorGoogleSheets instance.
        read_all: Reads all data from a Google Sheets document.

    """
    try:
        my_client = Client()
    except OSError as e:
        my_client = None

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.default_key = None
        self.path_root = kwargs.get('path_root')
        self.client = self.my_client if self.path_root is not None \
            else None

    def read_all(self, sheet_id: str, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read all data from a Google Sheets document.

        Args:
            sheet_id (str): The ID of the Google Sheets document.
            columns_date: array of columns to convert to datetime
            columns_numeric: array of columns to convert to numeric

        Returns:
            pd.DataFrame or None: The data read from the Google Sheets document, or None if the document is not found.
        """
        if self.client is None:
            url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export'
            df = pd.read_excel(url, **kwargs)
            df.columns.values[1] = "value"
            df.rename(columns={'Date': 'date'}, inplace=True)
            return df
        try:
            spread = Spread(sheet_id)
            df = spread.sheet_to_df()
            if df.index.name == 'date':
                df.reset_index(inplace=True)
#TODO: pass types from outside
            for column in df.columns:
                if column in kwargs.get('columns_date', []):
                    df[column] =  pd.to_datetime(df[column])
                if column in kwargs.get('columns_numeric', []):
                    df[column] = pd.to_numeric(df[column])
            return df
        except gspread.exceptions.SpreadsheetNotFound:
            return None

    def write_all(self, df, *args, **kwargs):
        key = kwargs.get('key', self.default_key)
        spread = Spread(key, create_spread=True)
        spread.move(self.path_root, create=True)
        spread.df_to_sheet(df, replace=(kwargs.get('if_exists', 'replace') == 'replace'))

cache_ = Cache()


def connector_factory(connector_type: str) -> Optional[Connector]:
    if connector_type.startswith('cache'):
        return cache_.connector()
    elif connector_type.startswith('object'):
        return ConnectorDirect()
    elif connector_type.startswith('csv'):
        if connector_type.startswith('csv:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorCsv(path_root=path_root)
        else:
            return ConnectorCsv()
    elif connector_type.startswith('gsheet'):
        if connector_type.startswith('gsheet:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorGoogleSheets(path_root=path_root)
        return ConnectorGoogleSheets()
    elif connector_type.startswith('json'):
        if connector_type.startswith('json:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorJson(path_root=path_root)
        else:
            return ConnectorJson()
    elif connector_type.startswith('playwright+http'):
        # return ConnectorRest(source_location, playwright=True)
        return ConnectorRest(playwright=True)
    elif connector_type.startswith('rest+http'):
        return ConnectorRest()
    elif connector_type.startswith('http'):
        return ConnectorRest(json=False)
    elif connector_type.startswith('sqlite') or \
            connector_type.startswith('postgresql') or \
            connector_type.startswith('mysql') or \
            connector_type.startswith('mariadb') or \
            connector_type.startswith('oracle') or \
            connector_type.startswith('mssql') or \
            connector_type.startswith('sqlalchemy') or \
            connector_type.startswith('gsheets') or \
            connector_type.startswith('pybigquery'):
        return ConnectorSql(connector_type)
    else:
        return None


def get_database_handle(db_type='mariadb+pymysql'):
    DB_USER = os.environ.get('DB_USER', None)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
    DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.environ.get('DB_PORT', None))
    DB_NAME = os.environ.get('DB_NAME', None)

    return f'{db_type}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

