#!/usr/bin/env python

import pandas as pd
from typing import Optional
from task import Task
from reader import ReaderRest, Reader
from writer import WriterCsv
from data import Data, DataJson, DataFormat
from transformer import Transformer
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


class DataPipeline:
    '''
    A pipeline class that
       multiple data sources can be added, each which are cleaned and processed.
       calculations can be performed on the ingested data
       data is exported to database
    '''
    def __init__(self):
        pass
        # read -> process -> QA
        # calculate -> QA
        # export
        self.ingestors = dict()
        self.transformers = dict()
        self.post_calc_qa = dict()
        self.exports = dict()

    # we should be able to assume that cleaned data exists in each of these ingestors, once we run it
    def create_ingestor(self, ingestor_name: str, reader: Reader) -> None:
        '''
        add self-contained imports, cleaning, and QA, yielding constrained data
        '''
        assert ingestor_name not in [name for name in self.ingestors.keys()], "ingestor name already used"
        assert not self.ingestors.get(ingestor_name, None), "ingestor name already used"
        self.ingestors[ingestor_name] = Ingestor(ingestor_name, reader)

    # todo -- it should be clear how data is fed into the transformer
    #      -- clearly map ingestors (feather files) to transformers
    # will there be 1 big transformer function or multiple? Likely one
    def add_transformer(self, transformer_name: str, transformer: Transformer) -> None:
        '''
        this takes the data in the data pipeline and processes it
        '''
        self.transformers[transformer_name] = transformer

    def remove_ingestor(self, ingestor_name: str) -> None:
        del self.ingestor[ingestor_name]

    def remove_transformer(self, transformer_name: str) -> None:
        del self.transformers[transformer_name]

    def run_pipeline(self)-> None:
        '''
        processes whole pipeline
        '''
        for name, ingestor in self.ingestors.items():
            res = ingestor.ingest()
            if res:
                raise Exception(f"error ingesting {name}")
        for transformer in self.transformers.keys():
            pass
            # we need to be able to cleanly label and process data


        # run QA
        pass

        # export data
        pass

foo = DataPipeline()
