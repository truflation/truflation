#!/usr/bin/env python3
"""
This script generates USD based indices from non USD indices
"""

import logging
import hashlib
from functools import partial
from icecream import ic

from dotenv import load_dotenv
import pandas as pd

from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import get_database_handle, connector_factory, ConnectorKwil

load_dotenv()
logger = logging.getLogger(__name__)
#ic.disable()

# Name
PIPELINE_NAME = "Generate USD"

TABLES = [
    'com_investing_lithium_carbonate_95_min_china_spot',
    'com_truflation_lithium_backfill',
    'com_investing_usd_cny_historical'
]

import hashlib

def transformer(inputs: dict) -> dict[pd.DataFrame]:
    ic(inputs)
    merged_tables = []
    for key, table in inputs.items():
        table['table'] = key
        merged_tables.append(table)
    return {
        'dump':
        pd.concat(
            merged_tables, ignore_index=True
        ).set_index([
            'table', 'date'
        ]).sort_index()
    }

def get_details(**config):
    sources = [
        SourceDetails(
            table,
            'kwil:',
            ConnectorKwil.get_hash(f'dev_{table}') + ":prices"
        ) for table in TABLES
    ]
    exports = [
        ExportDetails(
            'dump',
            'csv:csv',
            'dump.csv',
            replace=True
        )
    ]

    table_list = {}
    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=transformer
    )

if __name__ == "__main__":
    from truflation.data.pipeline import Pipeline
    logging.basicConfig(level=logging.DEBUG)
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
