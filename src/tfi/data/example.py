#!/usr/bin/env python3


from reader import Reader, ReaderCsv, ReaderSpecializedCsv
from data_pipeline import DataPipeline

my_pipeline = DataPipeline()

# will this be paired with one single csv or open to all?
reader = ReaderSpecializedCsv("example.csv")

my_pipeline.create_ingestor("cat_data", reader)
my_pipeline.ingestors["cat_data"].initialize()
# print(reader.read_all())

my_pipeline.run_pipeline()

# print(my_pipeline)
# print(reader)
