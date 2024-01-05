from typing import Dict
from datetime import datetime, timedelta
from icecream import ic
import pandas as pd
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import get_database_handle
from dotenv import load_dotenv

load_dotenv()
ic.disable()
PIPELINE_NAME = "Investing Com"
SOURCE_KEY = 'com.investing.pairid'

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
    db_handle = get_database_handle()
    metadata = Metadata(db_handle)
    data_list = metadata.read_by_key(SOURCE_KEY)
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
            name,
            db_handle,
            name
        )
        for name in data_list
    ]

    return  PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=lambda x: x
    )

if __name__ == "__main__":
    get_details()
