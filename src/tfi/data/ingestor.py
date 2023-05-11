#!/usr/bin/env python

from reader import Reader
import subprocess
import os
import shutil


class Ingestor:
    '''
    A pipeline class that
       imports data from API, Excel, or TruData
       Cleans it
       Computes data types and constraints
       Misc test
       Logs progress
       Returns pandas
    '''

    def __init__(self, name: str, reader: Reader):
        self.reader = reader
        # Name of instance
        self.name: str = ""
        # data file => self.feather_file
        # constraint file => self.tdda_file
        # results file => self.results_file

    def create_constraints(self):
        '''
        Automatically create constraints based on feather file, saved in self.feather_file
        '''
        if not self.name:
            raise Exception('Reader must have a name before creating constraint.')
        if os.path.isfile(f'{self.feather_file}'):
            shutil.copyfile(f'{self.feather_file}', f'{self.feather_file}.bak')
        cmd = f'tdda detect {self.feather_file} {self.tdda_file}'
        # todo -- use other module for running commands that logs
        subprocess.run(cmd)

    def load_constraints(self, constraint_path):
        assert os.path.isfile(constraint_path)
        shutil.copyfile(constraint_path, f'{self.feather_file}')

    def initialize(self):
        pass
        # read data from reader, receive dataframe
        df = self.reader.read_all()

        # save dataframe to feather files => name.feather
        self.save_df_to_feather(df)

        # todo -- run through parser
        # assume data already parsed

        # Create constraints based on data
        self.create_constraints()

        # delete feather file
        os.remove(f'{self.feather_file}')

        # user can now edit .tdda file to further

    def ingest(self):
        assert self.name != "", 'no name for ingestor'
        assert os.path.isfile(f'{self.feather_file}'), f'no constraint file'

        # read data from reader, receive dataframe
        df = self.reader.read_all()

        # save dataframe to feather files => name.feather
        self.save_df_to_feather(df)

        cmd = f'tdda detect {self.feather_file} {self.tdda_file} {self.results_file}'
        subprocess.run(cmd)

        # todo -- process results, which are in name.results

        # todo -- manual check

    def save_df_to_feather(self, df):
        df.to_feather(f'{self.feather_file}')

    @property
    def feather_file(self):
        return f'{self.name}.feather'

    @property
    def tdda_file(self):
        return f'{self.name}.tdda'

    @property
    def results_file(self):
        return f'{self.name}.results'
