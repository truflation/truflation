"""
Reader
"""

# Constarints library
# https://tdda.readthedocs.io/en/latest/constraints.html

import os
from typing import Optional
import pandas
import requests
import subprocess

from sqlalchemy import create_engine
from tfi.data.data import Data, DataPandas, DataJson

class Reader:
    """
    Base class for Import
    """
    def __init__(self):
        pass
        # we might use the convention of name.tdda, name.db for file paths
        # The types of data, confined to MariaDB types
        self.tdda_file: str = ""
        # Dataframe of processed data
        self.data_file: str = ""
        # Function to parse or transform data received form API to wanted format
        self.parser: object = None
        # Name of instance
        self.name: str = ""

    def authenticate(self, token):
        pass

    # todo -- @joseph this should return a standard format
    def read_all(
            self,
            *args,
            **kwargs
    ) -> Optional[Data]:
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

    def create_constraints(self):
        pass
        if not self.name:
            raise Exception('Reader must have a name before creating constraint.')
        elif not self.data:
            raise Exception(f'{self.name} (reader) must have a data before creating constraint.')

        cmd = f'tdda detect {self.data_file} {self.name}.tdda'
        # todo -- use other module for running commands that logs
        subprocess.run(cmd)

    def load_constraints(self, constraint_path):
        self.tdda_file = constraint_path

    def initialize(self):
        pass
        # todo -- read data
        # todo -- run through parser
        # todo -- store in self.name.datatype
        # self.create_constraints()
        self.create_constraints()
        # user can now edit .tdda file to further

    def ingest(self):
        assert self.tdda_file != "" and self.data_file != ""
        # todo -- read
        cmd = f'tdda detect {self.data_file} constraints.tdda {self.name}.results'
        subprocess.run(cmd)
        # todo -- process results
        # todo -- manual check
        pass

class ReaderCsv(Reader):
    def __init__(self):
        super().__init__()

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        df = pandas.read_csv(args[0])
        return DataPandas(df)

class ReaderSql(Reader):
    def __init__(self, engine):
        super().__init__()
        self.engine = create_engine(engine)

    def read_all(
            self,
            *args, **kwargs) -> Optional[Data]:
        df = pandas.read_sql(args[0], self.engine)
        return DataPandas(df)

class ReaderRest(Reader):
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
