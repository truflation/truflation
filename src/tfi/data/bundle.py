"""
Data bundle
"""

from enum import Enum

class BundleFormat(Enum):
    PANDAS = 1

class Bundle:
    def __init__(self):
        pass

    def set(self, format: BundleFormat, data):
        pass

    def get(self, format: BundleFormat):
        pass

class TimeSeries(Bundle):
    def __init__(self):
        pass
    

class TimeSeriesPandas(TimeSeries):
    def __init__(self, df):
        self.df = df

    def set(self, format: BundleFormat, df):
        self.df = df

    def get(self, format: BundleFormat):
        if format == BundleFormat.PANDAS:
            return self.df

class TimeSeriesJson(TimeSeries):
    def __init__(self, json):
        self.json = json

    def set(self, format: BundleFormat, df):
        self.json = json

    def get(self, format: BundleFormat):
        if format == BundleFormat.JSON:
            return self.json
