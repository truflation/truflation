from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.data import DataPandas, DataFormat
from tfi.data.details import PipeLineDetails

# self.name = None
# self.pre_ingestion_function = None
# self.post_ingestion_function = None
# self.sources = None
# self.transformer = lambda x: x
# self.export = None

# Name
name = "Hello World"


# self.pre_ingestion_function
def pre_ingestion_function():
    print(f'I do this before Ingestion')


# self.post_ingestion_function
def post_ingestion_function():
    print(f'I do this after Ingestion')


sources = {
    's1': "example.csv",
    's2': "example_2.csv"
}


def transformer(x):
    # df1 = self.reader.read_all(
    #     key="developer_hours"
    # ).get(DataFormat.PANDAS)
    # df2 = self.reader.read_all(
    #     key="developer_hours2"
    # ).get(DataFormat.PANDAS)
    # res_df = df1.copy()
    # res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
    # self.writer.write_all(DataPandas(res_df), key="hours_coding")
    return x


def exporter():
    print('to do')


def main():
    my_pipeline = PipeLineDetails(name=name,
                                  sources=sources,
                                  exporter=exporter,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    return my_pipeline


if __name__ == "__main__":
    main()
