import logging
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import yfinance as yf
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import Connector, get_database_handle
from typing import Dict, Any
load_dotenv()

# get the data from yahoo.  Skip the last date because yahoo
# will send live data
class YahooFinanceConnector(Connector):
    def read_all(self, *args, **kwargs):
        yesterday = datetime.utcnow().date() - relativedelta(days=1)
        data_frame = yf.download(
            args[0], **kwargs
        ).reset_index()
        return data_frame[
            ['Date', 'Adj Close']
        ].rename(columns={
            "Date": "date",
            "Adj Close": "value"
        }).set_index('date').round({
            "value": 2
        }).query('date <= @yesterday')

# Name
PIPELINE_NAME = "Yahoo Finance"
connector = YahooFinanceConnector()
SOURCE_KEY = 'com.yahoo.finance.pairid'

def transformer(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Function to transform the data dictionary.

    Args:
        data_dict (Dict[str, Any]): Input data dictionary.

    Returns:
        Dict[str, Any]: Transformed data dictionary.
    """
    return data_dict


def get_details() -> PipeLineDetails:
    """
    Function to get the pipeline details.

    Returns:
        PipeLineDetails: Pipeline details object.
    """
    db_handle = get_database_handle()
    metadata = Metadata(db_handle)
    data_list = metadata.read_by_key(SOURCE_KEY)

    sources = [
        SourceDetails(
            name,
            "override",
            source_id,
            connector=connector
        )
        for name, source_id in data_list.items()
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
        transformer=transformer
    )

if __name__ == "__main__":
    get_details()
