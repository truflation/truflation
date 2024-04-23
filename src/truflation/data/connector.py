"""
Connector
"""

import os
import json
import io
from icecream import ic
from typing import Optional, Iterator, Any, List
from pathlib import Path
import logging
import pandas as pd
import pandas_datareader.data as web
import gspread
from gspread_pandas import Spread, Client
import requests
from playwright.sync_api import sync_playwright

from truflation.data.logging_manager import Logger


import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, MetaData

logger = logging.getLogger(__name__)

def playw_browser():
    if playw_browser.count >= 50 :
        playw_browser.count = 0
        playw_browser.browser.close()
        playw_browser.playwright.stop()
        playw_browser.playwright = None
        playw_browser.browser = None
    if playw_browser.playwright is None:
        launch_params = {}
        playw_browser.playwright = sync_playwright().start()
        if os.environ.get('PROXY_SERVER') is not None and \
           os.environ.get('PROXY_USERNAME') is not None and \
           os.environ.get('PROXY_PASSWORD') is not None:
            launch_params = {
                'proxy': {
                    'server': os.environ.get('PROXY_SERVER'),
                    'username': os.environ.get('PROXY_USERNAME'),
                    'password': os.environ.get('PROXY_PASSWORD')
                }
            }
            ic('#### using proxy server with playwright ####')
        playw_browser.browser = playw_browser.playwright.firefox.launch(
            **launch_params
        )
        playw_browser.count += 1
    return playw_browser.browser

playw_browser.playwright = None
playw_browser.browser = None
playw_browser.count = 0

class Connector:
    """
    Base class for Import
    """

    def __init__(self, *_args, **_kwargs):
        self.logging_manager = Logger()
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

    def clear(self):
        self.cache_data.clear()


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

        if not len(args):
            raise Exception("need to specify source")
        source = args[0]
        if not source:
            raise Exception("sourc can not be falsey")

        if 'http:' in source or 'https:' in source[:6]:
            # Read the CSV data from the URL
            self.logging_manager.log_info('Reading CSV data...')
            
            try:
                df = pd.read_csv(source, **kwargs)
                self.logging_manager.log_info(f'CSV data successfully read from {source}')
                return df
            except Exception as e:
                self.logging_manager.log_error(f'Error reading CSV data from {source}: {e} ')
                return None

        filename = os.path.join(self.path_root, args[0])
        self.logging_manager.log_info(f'Reading CSV data from file: {filename}')
        self.logging_manager.log_debug(f'args[0]: {args[0]}')
        self.logging_manager.log_debug(f'filename: {filename}')
        # print(f'args[0]: {args[0]}')
        # print(f'filename: {filename}')
        if os.access(filename, os.R_OK):
            try:
                df = pd.read_csv(filename, dtype_backend='pyarrow', **kwargs)
                self.logging_manager.log_info(f'CSV data successfully read from file.')
                return df
            except Exception as e:
                self.logging_manager.log_error(f'Error reading CSV data from file: {e}')
                return None
        else:
            self.logging_manager.log_warning(f'File {filename} is not accessible.')
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
        self.logging_manager.log_info('Saving CSV data...')
        
        filename = kwargs.get('key', None)
        if_exists = kwargs.get('if_exists', 'none')
        
        if filename is None and len(args) > 0:
            filename = args[0]
            
        filename = os.path.join(self.path_root, filename)
        self.logging_manager.log_debug(f'File path: {filename}')
        
        if not os.path.exists(filename):
            self.logging_manager.log_info(f'File {filename} does not exist. Creating a new file.')
            return data.to_csv(
                filename
            )
            
        if if_exists == 'append':
            self.logging_manager.log_info(f'Appending data to file {filename}.')
            return data.to_csv(
                filename, mode='a', header=False
            )
            
        if if_exists == 'replace':
            self.logging_manager.log_info(f'Replacing file {filename} with new data.')
            return data.to_csv(
                filename
            )
        
        self.logging_manager.log_error("Invalid value for 'if_exists' parameter.")
        raise ValueError("Invalid value for 'if_exists' parameter.")

class ConnectorPandasDataReader(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()

    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        self.logging_manager.log_info('Reading data from web...')
        
        try:
            data = web.DataReader(*args[0])
            self.logging_manager.log_info('Data successfully retrieved from web.')
            return data
        except Exception as e:
            self.logging_manager.log_error(f'Error reading data from web: {e}')
            return None

    def write_all(self, data, *args, **kwargs) -> None:
        raise ValueError

class ConnectorPandasDataReader(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()

    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        return web.DataReader(*args[0])

    def write_all(self, data, *args, **kwargs) -> None:
        raise ValueError


class ConnectorJson(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_root = kwargs.get('path_root', os.getcwd())
        Path(self.path_root).mkdir(parents=True, exist_ok=True)

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        self.logging_manager.log_info('Loading JSON file...')
        
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
            
        if isinstance(filename, str):
            filepath = os.path.join(self.path_root, filename)
            self.logging_manager.log_info(f'Loading JSON file from: {filepath}')
            try:
                with open(filepath) as fileh:
                    obj = json.load(fileh)
                    self.logging_manager.log_info('JSON file loaded successfully.')
                    return obj
            except FileNotFoundError:
                self.logging_manager.log_error(f'File not found: {filepath}')
            except Exception as e:
                self.logging_manager.log_error(f"Error loading JSON file: {e}")
                return None
        else:
            return json.load(filename)

    def write_all(
            self,
            data,
            *args, **kwargs) -> None:
        self.logging_manager.log_info('Saving data to file...')
        
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
            
        if isinstance(filename, str):
            filename = os.path.join(self.path_root, filename)
            self.logging_manager.log_info(f'Writing data to file: {filename}')
            try:
                with open(filename, 'w') as fileh:
                    fileh.write(json.dumps(data, default=str))
                self.logging_manager.log_info('Data successfully written to file.')
            except Exception as e:
                self.logging_manager.log_error(f'Error writing data to file: {e}')
        else:
            if isinstance(data, str):
                self.logging_manager.log_info('Writing string data to file.')
                print(data, file=filename)
            elif isinstance(data, pd.DataFrame):
                self.logging_manager.log_info('Writing DataFrame to file')
                filename.write(data.to_json(**kwargs))
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
        self.logging_manager.log_info('Validating data type...')
        
        data_type = kwargs.get('data_type', None)
        data = kwargs.get('data', None)
        
        try:
            assert data_type is not None, "no data type specified"
            assert data is not None, "no data provided"
            assert isinstance(data, data_type), "data does not match data_type"
            self.logging_manager.log_info('Data type validation successful.')
            return data

        except AssertionError as e:
            self.logging_manager.log_error(f'Data type validation failed: {e}')
            return None

    def write_all(
            self,
            _,
            *_args, **_kwargs) -> None:
        raise NotImplementedError


class ConnectorSql(Connector):
    engines = {}

    def __init__(self, engine):
        super().__init__()
        self.engine = self.engines.get(engine)
        if self.engine is None:
            self.engine = \
                create_engine(engine, pool_pre_ping=True)
            self.engines[engine] = self.engine

    # rollbacks are necessary to prevent timeouts
    # see https://stackoverflow.com/questions/58378708/sqlalchemy-cant-reconnect-until-invalid-transaction-is-rolled-back
    # with error Can't reconnect until invalid transaction is rolled back.  Please rollback() fully before proceeding (Background on this error at: https://sqlalche.me/e/20/8s2b)
    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        self.logging_manager.log_debug('Executing SQL query...')
        
        with self.engine.connect() as conn:
            try:
                result_df = pd.read_sql(args[0], conn, dtype_backend='pyarrow', **kwargs)
                self.logging_manager.log_debug('SQL query executed successfully.')
                return result_df
            except Exception as e:
                self.logging_manager.log_debug(f'Error executing SQL query: {e}')
                conn.rollback()
                return None

    def write_all(
            self,
            data,
            *args,
            **kwargs
    ) -> None:
        self.logging_manager.log_info('Saving data to SQL database...')
        
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
                self.logging_manager.log_info('Data saved to SQL database successfully.')
            except Exception as e:
                self.logging_manager.log_exception(f'Error saving data to SQL database: {e}')
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
        self.logging_manager.log_info(f"Dropping table '{table_name}'...")
        
        try:
            tbl = Table(
                table_name, MetaData(),
                autoload_with=self.engine
            )
        except sqlalchemy.exc.NoSuchTableError:
            if ignore_fail:
                self.logging_manager.log_warning(f"Table '{table_name}' not found, skipping drop operation.")
                return
            else:
                self.logging_manager.log_error(f"Table '{table_name}' not found.")
                raise
        
        try:
            tbl.drop(self.engine, checkfirst=False)
            self.logging_manager.log_info(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            self.logging_manager.log_error(f"Error dropping table '{table_name}': {e}")
            raise

    def create_table(
            self,
            table_name: str,
            columns,
            **params):
        self.logging_manager.log_info(f"Creating table '{table_name}'...")
        
        metadata = MetaData()
        self.logging_manager.log_debug(f'Columns: {columns}')
        print(columns)
        
        table = Table(table_name, metadata, *columns)
        
        try:
            metadata.create_all(self.engine, **params)
            self.logging_manager.log_info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            self.logging_manager.log_exception(f"Error creating table '{table_name}': {e}")
            raise

class ConnectorRest(Connector):
    def __init__(self, **kwargs):
        super().__init__()
        self.playwright = kwargs.get('playwright', False)
        self.json = kwargs.get('json', True)
        self.csv = kwargs.get('csv', False)
        self.no_cache = kwargs.get('no_cache', False)
        self.page = None

    def read_all(
            self,
            url, *args, **kwargs) -> Any:
        self.logging_manager.log_info(f'Fetching data from URL: {url}')
        
        if self.playwright:
            try:
                if self.no_cache:
                    with sync_playwright() as p:
                        browser_type = p.firefox
                        browser = browser_type.launch()
                        self.page = browser.new_page()
                        response = self.page.goto(
                            url
                        )
                        self.logging_manager.log_info('Data fetched using Playwright.')
                        return self.process_response(response)

                with playw_browser().new_context() as context:
                    page = context.new_page()
                    response = page.goto(
                        url
                    )
                    self.logging_manager.log_info('Data fetched using Playwright.')
                    return self.process_response(response)
            except Exception as e:
                self.logging_manager.log_error(f'Error fetching data using Playwright: {e}')

        try:
            response = requests.get(os.path.join(url))
            if response.status_code == 200:
                self.logging_manager.log_info("Data fetched successfully using requests.")
                return self.process_response(response)
            else:
                self.logging_manager.log_warning(f'Unexpected status code {response.status_code} received from server.')
        except requests.exceptions.RequestException as e:
            self.logging_manager.log_error(f'Error fetching data using requests: {e}')
            return None
            
    def process_response(self, response):
        content_type = response.headers.get('content-type')
        self.logging_manager.log_info(f'Content type of response: {content_type}')
        
        if self.csv or content_type.startswith('text/csv'):
            self.logging_manager.log_info('Parsing response as CSV...')
            return pd.read_csv(
                io.StringIO(response.content.decode('utf-8')),
                dtype_backend='pyarrow'
            )
        if self.json or content_type.startswith('application/json'):
            self.logging_manager.log_info('Parsing response as JSON...')
            return self.process_json(response.json())
        if content_type == 'application/vnd.ms-excel' or \
                content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            self.logging_manager.log_info('Parsing response as Excel file...')
            return pd.read_excel(
                io.StringIO(response.content.decode('utf-8')),
                dtype_backend='pyarrow'
            )
        
        self.logging_manager.log_info('Parsing response content...')
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
            self.logging_manager.log_info(f'Fetching data from Google Sheets URL: {url}')
            try:
                df = pd.read_excel(url, dtype_backend='pyarrow', **kwargs)
                df.columns.values[1] = "value"
                df.rename(columns={'Date': 'date'}, inplace=True)
                return df
            except Exception as e:
                self.logging_manager.log_error(f'Error fetching data from Google Sheets: {e}')
                return None
        try:
            self.logging_manager.log_info('Fetching data from Google Sheets using gspread...')
            spread = Spread(sheet_id)
            columns_numeric = kwargs.get('columns_numeric', [])
            columns_float = kwargs.get('columns_float', [])
            columns_int = kwargs.get('columns_int', [])
            try:
                df = spread.sheet_to_df(unformatted_columns=columns_numeric + columns_float + columns_int)
            except AttributeError:
                df = spread.sheet_to_df()
            if df.index.name == 'date':
                df.reset_index(inplace=True)
            # TODO: pass types from outside
            for column in df.columns:
                if column in kwargs.get('columns_date', []):
                    df[column] = pd.to_datetime(df[column])
                if column in columns_numeric:
                    df[column] = pd.to_numeric(df[column])
                if column in columns_float:
                    df[column] = df[column].astype(float)
                if column in columns_int:
                    df[column] = df[column].astype(int)
            return df

        except gspread.exceptions.SpreadsheetNotFound:
            self.logging_manager.log_error('Google Sheets spreadsheet not found.')
            return None

    def write_all(self, df, *args, **kwargs):
        key = kwargs.get('key', self.default_key)
        spread = Spread(key, create_spread=True)
        spread.move(self.path_root, create=True)
        replace = kwargs.get('if_exists', 'replace') == 'replace'
        if replace:
            self.logging_manager.log_info('Replacing existing sheet with new data.')
            spread.df_to_sheet(df.astype(str), replace=replace)
        else:
            dims = spread.get_sheet_dims()
            self.logging_manager.log_debug(f'Sheet dimensions: {dims}')
            logger.debug(dims)
            start_row = dims[0] + 1 if dims[0] > 1 else 1
            self.logging_manager.log_info(f'Appending data to sheet starting from row {start_row}.')
            spread.df_to_sheet(df.astype(str), start=(start_row, 1), headers=dims[0] < 2)

class ConnectorExcel(Connector):
    """
    A class for connecting and reading data from Excel Sheets.

    Attributes:
        path_root: The root path for the Google Sheets document.

    Methods:
        __init__: Initializes the ConnectorGoogleSheets instance.
        read_all: Reads all data from a Google Sheets document.

    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.default_key = None
        self.path_root = kwargs.get('path_root')
        # self.client = self.my_client if self.path_root is not None \
        #     else None

    def read_all(self, source: str, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read all data from Excel.

        Args:
            source (str): The URL or absolute path to the excel file

        Returns:
            pd.DataFrame or None: The data read from the Google Sheets document, or None if the document is not found.
        """
        # todo -- this seems to work already regardless of being a link or path
        sheet_name = kwargs.get('sheet_name', None)  # specify which sheet to read in. Defaults to first

        # if "http" == source[:4]:
        # df = pd.read_excel(source, **kwargs) # **kwargs may include values not intended for this function

        self.logging_manager.log_info(f'Reading Excel file from: {source}')
        if source.endswith(".xls"):
            self.logging_manager.log_debug('Detected .xls file format.')
            df = pd.read_excel(
                source, engine='xlrd',
                dtype_backend='pyarrow',
                **kwargs) # needed for .xls files -- not xlsx
            # df = pd.read_excel(source, engine='openpyxl', **kwargs) # use for xlsx files (default?)
        else:
            self.logging_manager.log_debug('Detected .xlsx file format.')
            df = pd.read_excel(source, dtype_backend='pyarrow', **kwargs)

        # print(f'df received: \n{df}')
        # print(f'\n\ncolumns: \n: {df.columns}')

        # i don't think we should automatically set 'value' to the first column. Pass in a flag to do this.
        # df.columns.values[1] = "value"
        df.rename(columns={'Date': 'date'}, inplace=True)
        self.logging_manager.log_info('Excel file successfully read.')
        return df

        # try:
        #     spread = Spread(sheet_id)
        #     columns_numeric = kwargs.get('columns_numeric', [])
        #     columns_float = kwargs.get('columns_float', [])
        #     columns_int = kwargs.get('columns_int', [])
        #
        #     if df.index.name == 'date':
        #         df.reset_index(inplace=True)
        #
        #     for column in df.columns:
        #         if column in kwargs.get('columns_date', []):
        #             df[column] =  pd.to_datetime(df[column])
        #         if column in columns_numeric:
        #             df[column] = pd.to_numeric(df[column])
        #         if column in columns_float:
        #             df[column] = df[column].astype(float)
        #         if column in columns_int:
        #             df[column] = df[column].astype(int)
        #     return df
        #
        # except gspread.exceptions.SpreadsheetNotFound:
        #     return None

    def write_all(self, df, *args, **kwargs):
        raise Exception("write_all not implemented for Excel connector")
        # key = kwargs.get('key', self.default_key)
        # spread = Spread(key, create_spread=True)
        # spread.move(self.path_root, create=True)
        # replace = kwargs.get('if_exists', 'replace') == 'replace'
        # if replace:
        #     spread.df_to_sheet(df.astype(str), replace=replace)
        # else:
        #     dims = spread.get_sheet_dims()
        #     logger.debug(dims)
        #     spread.df_to_sheet(df.astype(str), start=(dims[0]+1 if dims[0]>1 else 1,1), headers=dims[0]<2)

        # todo -- adapt csv write_all to Excel
        # filename = kwargs.get('key', None)
        # if_exists = kwargs.get('if_exists', 'none')
        # if filename is None and len(args) > 0:
        #     filename = args[0]
        # filename = os.path.join(self.path_root, filename)
        # if not os.path.exists(filename):
        #     return data.to_csv(
        #         filename
        #     )
        # if if_exists == 'append':
        #     return data.to_csv(
        #         filename, mode='a', header=False
        #     )
        # elif if_exists == 'replace':
        #     return data.to_csv(
        #         filename
        #     )
        # else:
        #     raise ValueError


cache_ = Cache()
connector_factory_list = []

# todo -- I think we should have a dedicated Connector for Excel, just as we do for csv
# I like have abstraction for http links, but I find it troublesome for its potential misinterpretation
def connector_factory(connector_type: str) -> Optional[Connector]:
    if connector_type.startswith('cache'):
        return cache_.connector()
    if connector_type.startswith('object'):
        return ConnectorDirect()
    if connector_type == 'excel':
        return ConnectorExcel()
    if connector_type == 'csv':
        return ConnectorCsv()
    if connector_type.startswith('csv:'):
        path_root = connector_type.split(':', 1)[1]
        return ConnectorCsv(path_root=path_root)
    if connector_type.startswith('gsheet'):
        if connector_type.startswith('gsheet:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorGoogleSheets(path_root=path_root)
        return ConnectorGoogleSheets()
    if connector_type.startswith('json'):
        if connector_type.startswith('json:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorJson(path_root=path_root)
        return ConnectorJson()
    if connector_type.startswith('playwright+http'):
        # return ConnectorRest(source_location, playwright=True)
        return ConnectorRest(playwright=True)
    if connector_type.startswith('rest+http'):
        return ConnectorRest()
    if connector_type.startswith('http'):
        return ConnectorRest(json=False)
    if connector_type.startswith('csv+http'):
        return ConnectorRest(csv=True)
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
    if connector_type.startswith('pandas_datareader'):
        return ConnectorPandasDataReader()
    for factory in connector_factory_list:
        result = factory(connector_type)
        if result is not None:
            return result
    return None

def add_connector_factory(factory_function) -> None:
    connector_factory_list.append(factory_function)

def get_database_handle(db_type='mariadb+pymysql'):
    DB_USER = os.environ.get('DB_USER', None)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
    DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.environ.get('DB_PORT', None))
    DB_NAME = os.environ.get('DB_NAME', None)

    return f'{db_type}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
