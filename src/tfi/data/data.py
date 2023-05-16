"""
Data bundle
"""

from typing import Optional
from enum import Enum
import pandas as pd

class DataFormat(Enum):
    PANDAS = 1
    JSON = 2

class Data:
    def __init__(self):
        pass

    def set(self, data, format_: DataFormat):
        pass

    def get(self, format_: DataFormat):
        pass

class DataPandas(Data):
    def __init__(self, df):
        super().__init__()
        self.df = df

    def set(self, data, format_: DataFormat = DataFormat.PANDAS ):
        self.df = data

    def get(self, format_: DataFormat = DataFormat.PANDAS):
        if format_ == DataFormat.PANDAS:
            return self.df
        return None

class DataJson(Data):
    def __init__(self, json):
        super().__init__()
        self.json = json

    def json2df(self) -> pd.DataFrame:
        raise NotImplementedError

    def set(self, data, format_: DataFormat = DataFormat.JSON) \
        -> None:
        self.json = data

    def get(self, format_: DataFormat = DataFormat.JSON) \
        -> Optional[object]:
        if format_ == DataFormat.JSON:
            return self.json
        if format_ == DataFormat.PANDAS:
            return self.json2df()
        return None
