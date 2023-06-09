import logging
import os
from dotenv import load_dotenv
import yfinance as yf
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import Connector

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_USER = os.environ.get('DB_USER', None)
DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = int(os.environ.get('DB_PORT', None))
DB_NAME = os.environ.get('DB_NAME', None)

# Name
pipeline_name = "Hello World"
source_key = 'com.google.sheets.gsheet'

def pre_ingestion_function():
    print(f'I do this before ingestion')


def post_ingestion_function():
    print(f'I do this after ingestion')


# Source Types: csv, API, Excel, Google Sheet, TrueData

sources = [
]

exports = [
]


def transformer(data_dict: dict):
    return data_dict

def get_details():
    db = f'mariadb+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    metadata = Metadata(db)
    datalist = metadata.read_by_key(source_key)
    logger.debug('Read data list')
    sources = [
        SourceDetails(
            name,
            "gsheets",
            gsheet
        )
        for name, gsheet in datalist.items()
    ]
    exports = [
        ExportDetails(
            name,
            db,
            name
        )
        for name in datalist
    ]

    cron_schedule = {
        "second": "15, 30, 45, 0",  # At the start of the minute
        "minute": "*",  # At the start of the minute
        "hour": "*",  # First hour
        "day": "*",  # On the csv_example day of the month
        "month": "*",  # In January
        "year": "*",  # In January
        # "day_of_week": "mon",  # On Mondays
    }


    my_pipeline = PipeLineDetails(name=pipeline_name,
                                  sources=sources,
                                  exports=exports,
                                  cron_schedule=cron_schedule,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    return my_pipeline


if __name__ == "__main__":
    get_details()
