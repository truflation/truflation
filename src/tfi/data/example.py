from reader import Reader, ReaderCsv
from data_pipeline import DataPipeline


my_pipeline = DataPipeline()

# will this be paired with one single csv or open to all?
reader = ReaderCsv()

my_pipeline.create_ingestor("cat_data", reader)
my_pipeline.ingestors["cat_data"].initialize()

print(my_pipeline)
print(reader)