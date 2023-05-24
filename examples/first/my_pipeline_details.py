from tfi.data.pipeline_details import PipeLineDetails
from tfi.data.source_details import SourceDetails
from tfi.data.export_details import ExportDetails
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv('DB_PASS')

# Name
pipeline_name = "Hello World"


def pre_ingestion_function():
    print(f'I do this before ingestion')


def post_ingestion_function():
    print(f'I do this after ingestion')


# Source Types: csv, API, Excel, Google Sheet, TrueData
sources = [
    # name, source_type, source, parser function (default is pass through))
    SourceDetails("first", "csv", "examples/first/example.csv"),
    SourceDetails("second", "csv", "examples/first/example_2.csv", lambda x: x)
]

exports = [
    ExportDetails(name='sum',
                  host ='api-test.truflation.io',
                  port=3306,
                  db='timeseries',
                  table='work_details',
                  username="root",
                  password=DB_PASS)
]


def transformer(data_dict: dict):
    df1 = data_dict['first']
    df2 = data_dict['second']
    res_df = df1.copy()
    res_df["value"] = df1["value"].add(df2["value"])

    res_dict = {"sum": res_df}
    return res_dict


def get_details():
    my_pipeline = PipeLineDetails(name=pipeline_name,
                                  sources=sources,
                                  exports=exports,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    return my_pipeline


if __name__ == "__main__":
    get_details()
