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
from typing import Callable

class DataPipeline:
    """
    A pipeline class that
       multiple data sources can be added.
       calculations can be performed on the ingested data
       data is exported to database
    """
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
    def create_ingestor(self, ingestor_name: str, reader: Reader, source: str, parser: Callable = lambda x: x) -> None:
        """
        add self-contained imports, cleaning, and QA, yielding constrained data

        Parameters
        ----------
            ingestor_name : str
                name of ingestor, used as reference
            reader : Reader
                Reader to be called for ingestion using read_all
            source : str
                source for reading data
            parser : Object
                Function to parse data
        """
        assert ingestor_name not in self.ingestors.keys(), "ingestor name already used"
        assert not self.ingestors.get(ingestor_name, None), "ingestor name already used"
        self.ingestors[ingestor_name] = Ingestor(ingestor_name, reader, source, parser)

    # todo -- This was changed from class Transformer to a function --- confirm choice
    def add_transformer(self, transformer: Transformer) -> None:
        """
        this takes the data in the data pipeline and processes it

        Parameters
        ----------
            transformer : Transformer
                function called that will be called with argument of self.ingestors
        """
        self.transformer  = transformer

    def remove_ingestor(self, ingestor_name: str) -> None:
        """
        Removes ingestor by name

        Parameters
        ----------
            ingestor_name : str
                name of ingestor, needed for reference in transformer
        """
        del self.ingestors[ingestor_name]

    # this will either be a transformer class or a function
    def remove_transformer(self) -> None:
        """
        Sets transformer to None
        """
        self.transformer = None

    def run_pipeline(self)-> None:
        """
        processes whole pipeline
        """
        print(f'\nIngesting...')
        for name, ingestor in self.ingestors.items():
            res = ingestor.ingest()
            if res:
                raise Exception(f"error ingesting {name}")
            # todo --- temp print out for fun
            print(f'\n{name}:')
            self.print_feather_file(ingestor.feather_file)

        print(f'\nTransforming...')
        # run tranformer
        if self.transformer:
            df = self.transformer(self.ingestors)
            print(df)
        print(f'\nQA...')
        # todo -- run QA
        pass

        print(f'\nExporting...')
        # todo -- export data
        pass

        # todo -- run some function after everything else has finished
        print(f'executing final')

    @staticmethod
    def print_feather_file(feather_file_path):
        """
        Temporary function to print out feather files.
        """
        print(f'v' * 48)
        print(pd.read_feather(feather_file_path))
        print(f'^' * 48)
