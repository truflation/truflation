from tfi.data.pipeline_details import PipeLineDetails
from tfi.data.source_details import SourceDetails

# Name
pipeline_name = "Hello World"


def pre_ingestion_function():
    print(f'I do this before ingestion')


def post_ingestion_function():
    print(f'I do this after ingestion')


# Source Types: csv, API, Excel, Google Sheet, TrueData
sources = [
    # name, source_type, source, parser function (default is pass through))
    SourceDetails("first", "csv", "examples/example.csv"),
    SourceDetails("second", "csv", "examples/example_2.csv", lambda x: x)
]


def transformer(data_dict: dict):
    df1 = data_dict['first']
    df2 = data_dict['second']
    res_df = df1.copy()
    res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])

    res_dict = {"sum": res_df}
    return res_dict


# todo --  ability to dump all or add row by row
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
    return my_pipeline


if __name__ == "__main__":
    get_details()
