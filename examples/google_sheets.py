import logging
import os

import pandas as pd
from dotenv import load_dotenv
import yfinance as yf
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import Connector, get_database_handle
from typing import Dict, Any
load_dotenv()

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

# Name
PIPELINE_NAME = "Google Sheets"
SOURCE_KEY = 'com.google.sheets.gsheet'

def parse_gsheet(sheet) -> pd.DataFrame:
    """
    Function to parse the Google Sheets json object.

    Args:
        sheet : Google sheets object.

    Returns:
        pd.DataFrame: Parsed dataframe from json.
    """
    sheet.rename(columns={
        sheet.columns[0]: 'date',
        sheet.columns[1]: 'value',
    },inplace=True)
    return sheet


def get_details() -> PipeLineDetails:
    """
    Function to get the pipeline details.

    Returns:
        PipeLineDetails: Pipeline details object.
    """
    db_handle = get_database_handle()
    metadata = Metadata(db_handle)
    data_list = metadata.read_by_key(SOURCE_KEY)
    logger.debug('Read data list')
    sources = [
        SourceDetails(
            name,
            "gsheets",
            gsheet,
            parser = parse_gsheet
        )
        for name, gsheet in data_list.items()
    ]
    exports = [
        ExportDetails(
            name,
            db_handle,
            name
        )
        for name in data_list
    ]

    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=lambda x: x
    )

if __name__ == "__main__":
    get_details()
