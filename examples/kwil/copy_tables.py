#!/usr/bin/env python3
"""
This copies data into kwil
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
from truflation.data.connector import get_database_handle
from truflation.data.connector import connector_factory
from truflation.data.connectors.kwil import ConnectorKwil

load_dotenv()
logger = logging.getLogger(__name__)
#ic.disable()

# Name
PIPELINE_NAME = "Generate USD"


ROUNDING = 6
TABLES = [
    'com_truflation_lithium_backfill',
    'com_investing_lithium_carbonate_95_min_china_spot',
    'com_investing_usd_cny_historical'
]

import hashlib

def get_details(**config):
    db_handle = config.get('db_handle', get_database_handle())
    sources = [
        SourceDetails(
            table,
            db_handle,
            table
        ) for table in TABLES
    ]
    exports = [
        ExportDetails(
            table,
            'kwil:',
            ConnectorKwil.get_hash(f'dev_{table}') + ":prices"
        ) for table in TABLES
    ]
    table_list = {}
    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports
    )

if __name__ == "__main__":
    from truflation.data.pipeline import Pipeline
    logging.basicConfig(level=logging.DEBUG)
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
