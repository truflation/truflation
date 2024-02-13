"""
Rest To CSV connector
"""

import requests
import pandas as pd
from truflation.data.connector import Connector

class RestToCsvConnector(Connector):
    def __init__(self, rest_url, csv_path):
        super().__init__()
        self.rest_url = rest_url
        self.csv_path = csv_path

    def fetch_data_from_rest(self):
        response = requests.get(self.rest_url)
        response.raise_for_status()
        return response.json()

    def read_all(self, *args, **kwargs) -> pd.DataFrame | None:
        data = self.fetch_data_from_rest()
        df = pd.DataFrame(data)
        return df

    def write_all(self, data, *args, **kwargs) -> None:
        data.to_csv(self.csv_path, index=False)
