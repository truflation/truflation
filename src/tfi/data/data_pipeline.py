#!/usr/bin/env python

import pandas as pd
from typing import Optional
from task import Task
from reader import ReaderRest
from writer import WriterCsv
from data import Data, DataJson, DataFormat
from transformer import Transformer


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

    def __init__(self, reader):
        pass
        self.reader = reader

    def add_test(self, test):
        pass


    def ingest(self):
        pass
        # self.reader.import()


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
    def add_ingestor(self, ingestor_name: str, ingestor: Ingestor) -> None:
        '''
        add self-contained imports, cleaning, and QA, yielding constrained data
        '''
        self.ingestors[ingestor_name] = ingestor

    # todo -- it should be clear how data is fed into the transformer
    #
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
            pass
            # res =  # ingestor.ingest
            # if res:
            #     #     error

        for transformer in self.transformers.keys():
            pass
            # we need to be able to cleanly label and process data


        # run QA

        # export data


foo = DataPipeline()
