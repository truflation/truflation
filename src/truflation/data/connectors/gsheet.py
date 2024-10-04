
import gspread
import pandas as pd

from typing import Optional
from gspread_pandas import Spread, Client
from truflation.data.connectors.base import Connector


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
            self.logger.debug(dims)
            start_row = dims[0] + 1 if dims[0] > 1 else 1
            self.logging_manager.log_info(f'Appending data to sheet starting from row {start_row}.')
            spread.df_to_sheet(df.astype(str), start=(start_row, 1), headers=dims[0] < 2)
