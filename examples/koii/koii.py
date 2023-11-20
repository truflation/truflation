#!/usr/bin/env python3

"""
This is a test file to load in koii data and ingest it
into the truflation data framework

Install the truflation package and then

pipeline_run_direct.py --debug koii.py
"""

from icecream import ic

from truflation.data.connector import get_database_handle
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from dotenv import load_dotenv
import os

load_dotenv()

pipeline_name = "KOII Test reader"


def pre_ingestion_function():
    ic('I do this before ingestion')

def post_ingestion_function():
    ic('I do this after ingestion')

def transformer(inputs: dict) -> dict:
    """ Convert koii json structure into a pandas data frame"""
    frame = inputs['koii_inputs']
    return {
        'outputs': frame
    }


def get_details(**config: dict):
    exports = [
        ExportDetails(
            'outputs',
            'csv:csv',
            'output.csv'
        )
    ]

    sources = [
        SourceDetails(
            'koii_input',
            'http',
            'https://example.com',
            parser=lambda x: x
        )
    ]

    my_pipeline = PipeLineDetails(name=pipeline_name,
                                  sources=sources,
                                  exports=exports,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    return my_pipeline

if __name__ == "__main__":
    Pipeline(get_details()).ingest()

  
