#!/usr/bin/env python

import pandas as pd
from typing import Optional
from task import Task
from reader import ReaderRest, Reader
from writer import WriterCsv
from data import Data, DataJson, DataFormat
from ingestor import Ingestor
from transformer import Transformer
import subprocess
import os
import shutil



class DataPipeline:
    '''
    A pipeline class that
       multiple data sources can be added.
       calculations can be performed on the ingested data
       data is exported to database
    '''
    def __init__(self):
        pass
        # read -> process -> QA
        # calculate -> QA
        # export
        self.ingestors = dict()
        self.transformer = None
        self.post_calc_qa = dict()
        self.exports = dict()

    # we should be able to assume that cleaned data exists in each of these ingestors, once we run it
    def create_ingestor(self, ingestor_name: str, reader: Reader) -> None:
        '''
        add self-contained imports, cleaning, and QA, yielding constrained data
        '''
        assert ingestor_name not in self.ingestors.keys(), "ingestor name already used"
        assert not self.ingestors.get(ingestor_name, None), "ingestor name already used"
        self.ingestors[ingestor_name] = Ingestor(ingestor_name, reader)

    # todo -- it should be clear how data is fed into the transformer
    #      -- clearly map ingestors (feather files) to transformers
    # will there be 1 big transformer function or multiple? Likely one
    def add_transformer(self, transformer: Transformer) -> None:
        '''
        this takes the data in the data pipeline and processes it
        '''
        self.transformer  = transformer

    def remove_ingestor(self, ingestor_name: str) -> None:
        del self.ingestors[ingestor_name]

    # this will either be a transformer class or a function
    def remove_transformer(self) -> None:
        self.transformer = None

    def run_pipeline(self)-> None:
        '''
        processes whole pipeline
        '''
        for name, ingestor in self.ingestors.items():
            res = ingestor.ingest()
            if res:
                raise Exception(f"error ingesting {name}")

        # run tranformer
        if self.transformer:
            self.transformer(self.ingestors)

        # run QA
        pass

        # export data
        pass

        # todo -- temp remove me
        print(f'Ingested Data: ')
        for name, ingestor in self.ingestors.items():
            print(f'{name}')
            print(pd.read_feather(ingestor.feather_file))

