from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.data import DataPandas, DataFormat
from tfi.data.pipeline_details import PipeLineDetails
from tfi.data.source_details import SourceDetails

# self.name = None
# self.pre_ingestion_function = None
# self.post_ingestion_function = None
# self.sources = None
# self.transformer = lambda x: x
# self.export = None

# Name
pipeline_name = "Hello World"
name = "DONKEY"

# class SourceDetails:
#     def __init__(self, name, source_type, source, parser=lambda x: x):
#         self.name = name
#         self.source_type = source_type
#         self.source = source
#         self.parser = parser


def pre_ingestion_function():
    print(f'I do this before Ingestion')


def post_ingestion_function():
    print(f'I do this after Ingestion')


# Source Types: csv, API, Excel, Google Sheet, TrueData
sources = [
    SourceDetails("first", "csv", "example.csv"),
    SourceDetails("second", "csv", "example_2.csv")
]


def transformer(data_dict: dict):
    pass
    return data_dict

    #
    # df1 = self.reader.read_all(
    #     key="developer_hours"
    # ).get(DataFormat.PANDAS)
    # df2 = self.reader.read_all(
    #     key="developer_hours2"
    # ).get(DataFormat.PANDAS)
    # res_df = df1.copy()
    # res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
    # self.writer.write_all(DataPandas(res_df), key="hours_coding")


def exporter():
    print('to do')


def get_details():
    my_pipeline = PipeLineDetails(name=pipeline_name,
                                  sources=sources,
                                  exporter=exporter,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    print(f'my_pipeline name: {my_pipeline.name}')
    return my_pipeline

if __name__ == "__main__":
    get_details()
