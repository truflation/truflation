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

from typing import Dict
import hashlib

from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import get_database_handle, connector_factory, ConnectorKwil

load_dotenv()
logger = logging.getLogger(__name__)
#ic.disable()

# Name
PIPELINE_NAME = "Generate USD"


ROUNDING = 6
TABLES = [
    'com_investing_dutch_ttf_natural_gas_futures',
    'com_investing_usd_eur_historical'
]
pairs = {
    'a': 'A',
    'b': 'B'
}

def parse_investing_com(json_obj: Dict) -> pd.DataFrame:
    """
    Function to parse investing.com json object.

    Args:
        json_obj (Dict): investing.com json object.

    Returns:
        pd.DataFrame: Parsed dataframe from json.
    """
    if 'data' not in json_obj:
        raise Exception("No data found in json_obj")

    def convdate(timestamp):
        return datetime.utcfromtimestamp(
            timestamp/1000
        )
    return_value = {
        'date': [],
        'value': [],
    }
    for obj in json_obj['data']:
        return_value['date'].append(convdate(obj[0]))
        return_value['value'].append(obj[4])
    data_frame = pd.DataFrame(
        return_value
    ).set_index('date')

    # pull data for yesterday
    cutoff_utc = datetime.utcnow()
    cutoff_utc = cutoff_utc - timedelta(days=1)
    cutoff_utc = cutoff_utc.date()
    data_frame = data_frame.loc[
        pd.to_datetime(data_frame.index).date <= cutoff_utc
    ]
    ic(data_frame)
    return data_frame


def get_details() -> PipeLineDetails:
    """
    Function to get the pipeline details.

    Returns:
        PipeLineDetails: Pipeline details object.
    """
    
    data_list = pairs
    sources = [
        SourceDetails(
            name,
            'playwright+http',
            f'https://api.investing.com/api/financialdata/{pair_id}/historical/chart?period=MAX&interval=P1D&pointscount=120',
            parser=parse_investing_com
        )
        for name, pair_id in data_list.items()
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
