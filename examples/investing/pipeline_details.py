import logging
import os
from datetime import datetime
import pandas as pd
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_USER = os.environ.get('DB_USER', None)
DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = int(os.environ.get('DB_PORT', None))
DB_NAME = os.environ.get('DB_NAME', None)


# Name
PIPELINE_NAME = "Hello World"
SOURCE_KEY = 'com.investing.pairid'


def pre_ingestion_function():
    print('I do this before ingestion')


def post_ingestion_function():
    print('I do this after ingestion')


def transformer(data_dict: dict):
    return data_dict


def parse_investing_com(json_obj) -> pd.DataFrame:
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
    logging.debug(data_frame)
    return data_frame


def get_details():
    db_handle = \
        f'mariadb+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    metadata = Metadata(db_handle)
    datalist = metadata.read_by_key(SOURCE_KEY)
    logger.debug('Read data list')
    sources = [
        SourceDetails(
            name,
            'playwright+http',
            f'https://api.investing.com/api/financialdata/{pair_id}/historical/chart?period=MAX&interval=P1D&pointscount=120',
            parser=parse_investing_com
        )
        for name, pair_id in datalist.items()
    ]
    exports = [
        ExportDetails(
            name,
            db_handle,
            name
        )
        for name in datalist
    ]

    my_pipeline = PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        pre_ingestion_function=pre_ingestion_function,
        post_ingestion_function=post_ingestion_function,
        transformer=transformer
    )
    return my_pipeline


if __name__ == "__main__":
    get_details()
