import pandas as pd
from typing import Optional
from .base import Connector


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
            df = pd.read_excel(
                source, engine='openpyxl', 
                dtype_backend='pyarrow', 
                **kwargs)

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

