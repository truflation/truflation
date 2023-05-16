#!/usr/bin/env python3

from tfi.data.reader import Reader, ReaderCsv, ReaderSpecializedCsv
from tfi.data.data_pipeline import DataPipeline
import pandas as pd

# create data pipeline
my_pipeline = DataPipeline()

# Create a reader that is linked to the file "example.csv"
csv_reader = ReaderSpecializedCsv()

# Create a ingestor based on our reader named 'cat_data'
my_pipeline.create_ingestor("developer_hours", csv_reader, source="example.csv")
# Load in developer_hours and automatically create a file called 'developer_hours.tnna' that guesses data constraints
my_pipeline.ingestors["developer_hours"].initialize()

# Create a ingestor based on our reader named 'developer_hours'
my_pipeline.create_ingestor("developer_hours_2", csv_reader, source="example.csv")
# Load in developer_hours_2 and automatically create a file called 'developer_hours_2.tnna' that guesses constraints
my_pipeline.ingestors["developer_hours_2"].initialize()


# todo -- use this type:
#      DataPandas(df) -->  <class 'data.DataPandas'>
def add_hours(my_ingestors: dict) -> pd.DataFrame:
    df1 = pd.read_feather(my_ingestors["developer_hours"].feather_file)
    df2 = pd.read_feather(my_ingestors["developer_hours_2"].feather_file)

    res_df = df1.copy()
    res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
    return res_df


# add transformer to combine ingested data
my_pipeline.add_transformer(add_hours)

# Run the whole data pipeline
my_pipeline.run_pipeline()
