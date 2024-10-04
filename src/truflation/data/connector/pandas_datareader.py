import pandas as pd
import pandas_datareader.data as web

from typing import Optional
from .base import Connector


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
