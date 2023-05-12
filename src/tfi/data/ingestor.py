#!/usr/bin/env python

from reader import Reader
import subprocess
import os
import shutil


# pip install tdda
# pip install pyarrow
# pip install feather-format

class Ingestor:
    """
    A pipeline class that
       imports data from API, Excel, or TruData
       Cleans it
       Computes data types and constraints
       Misc test
       Logs progress
       Returns pandas
    """

    def __init__(self, name: str, reader: Reader):
        """
        initiates object of type Ingestor

        Parameters
        ----------
            name : str
                name of ingestor, needed for reference in transformer
        """

        self.reader = reader
        # Name of instance
        self.name: str = name
        # data file => self.feather_file
        # constraint file => self.tdda_file
        # results file => self.results_file

    def create_constraints(self):
        """
        Automatically create constraints based on feather file, saved in self.feather_file
        """
        if not self.name:
            raise Exception('Reader must have a name before creating constraint.')
        if os.path.isfile(f'{self.tdda_file}'):
            shutil.copyfile(f'{self.tdda_file}', f'{self.tdda_file}.bak')
        cmd = f'tdda discover {self.feather_file} {self.tdda_file}'
        # todo -- use other module for running commands that logs
        subprocess.run(cmd, shell=True)

    def load_constraints(self, constraint_path):
        """
        Duplicates existing constraint file of type tdda

        Parameters
        ----------
            constraint_path : str
                path of constraint file
        """
        assert os.path.isfile(constraint_path)
        shutil.copyfile(constraint_path, f'{self.feather_file}')

    def initialize(self):
        """
        Initialize already created Ingestor object by reading in data from Reader and automatically creating a constraint file
        """
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
        """ Ingests data and checks constraints """

        assert self.name != "", 'no name for ingestor'
        assert os.path.isfile(f'{self.tdda_file}'), f'no constraint file'

        # read data from reader, receive dataframe
        df = self.reader.read_all()

        # save dataframe to feather files => name.feather
        self.save_df_to_feather(df)

        # todo -- i think detecting and verifying do the same thing. Detecting gives less details
        # "verifying"
        cmd = f'tdda verify {self.feather_file} {self.tdda_file}'
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
        # print(res)
        passed = self.check_if_passed_constraints(res.stdout.decode('utf-8'))
        print(f'Ingestor verify test has {"passed" if passed else "failed"}')
        assert passed is True

        # "detecting"
        cmd = f'tdda detect {self.feather_file} {self.tdda_file} {self.results_file}'
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
        output = res.stdout.decode('utf-8')
        passed = self.check_if_passed_constraints(res.stdout.decode('utf-8'))
        print(f'Ingestor verify test has {"passed" if passed else "failed"}')
        assert passed is True

        # todo -- manual check

    @staticmethod
    def check_if_passed_constraints(results: str) -> bool:
        """
        Takes string of results and parses it to show number of failed tests, returning

        Parameters
        ----------
            results : str
                path of constraint file

        :returns: bool: true if passed all, false otherwise
        """

        results_list = results.split('\n')
        failing_str = next(x for x in results_list if "Constraints failing: " in x)
        num_failed = int(failing_str.split(": ")[1])
        return num_failed == 0

    def save_df_to_feather(self, df):
        """Saves dataframe type as [name].feather"""
        df.to_feather(f'{self.feather_file}')

    @property
    def feather_file(self):
        """Name of feather file"""
        return f'{self.name}.feather'

    @property
    def tdda_file(self):
        """Name of tdda file"""
        return f'{self.name}.tdda'

    @property
    def results_file(self):
        """Name of results file"""
        return f'{self.name}.results'
