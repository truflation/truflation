import datetime

from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.general_loader import GeneralLoader
# from tfi.data.data import DataPandas, DataFormat
from tfi.data.pipeline_details import PipeLineDetails
from tfi.data.exporter import Exporter


# todo -- later, create a new pipeline to process al data in parallel -- new class


class Pipeline(Task):
    def __init__(self, pipeline_details: PipeLineDetails):
        # super().__init__(reader, writer)
        self.name = pipeline_details.name
        self.pre_ingestion_function = pipeline_details.pre_ingestion_function,
        self.post_ingestion_function = pipeline_details.post_ingestion_function,
        self.sources = dict({x.name: x for x in pipeline_details.sources})
        self.loader = GeneralLoader()
        # self.validator = Validator(self.reader, self.writer)  # todo -- this should be general purpose
        self.transformer = pipeline_details.transformer
        self.exports = pipeline_details.exports
        self.exporter = Exporter()


    def ingest(self) -> None:
        # todo -- create try except after functionality works
        self.header("Ingesting...")

        # Pre-Ingestion Function
        self.header("Pre Ingestion Function...")
        self.pre_ingestion_function[0]()  # The class saves the function as a tuple

        # Read, Parse,  and Validate from all sources
        self.header("Loading...")
        for source_name, source_details in self.sources.items():
            self.loader.run(source_details, source_name)

        # Transform x sources into y dataframes
        self.header("Transforming...")
        self.loader.transform(self.transformer)

        # Get cache
        my_cache = self.loader.cache

        #  Export y dataframes into z tables on servers
        self.header("Exporting...")
        for export_details in self.exports:
            self.exporter.export(export_details, my_cache.get(export_details.name, None))

        #  Post ingestion function
        self.header("Post Ingestion Function...")
        self.post_ingestion_function[0]()

        self.header("TESTING FROZEN DATA...")
        df_frozen = self.exporter.get_frozen_data(self.exports[0], frozen_datetime=datetime.datetime.now() - datetime.timedelta(days=5)) # Timestamp for may 19 2023
        print(f'df_frozen: \n{df_frozen}')

    @staticmethod
    def header(s: str):
        print('\n' + f'#'*20 + f'   {s}   ' + '#'*(20 - len(s)))

# Create docker setup -- David
# Create exporter class functionality -- David
# Set up server-side SQL tables and login ---- @Joseph
# Deploy everything --- ? together where -- production? and test?? @Joseph, @David
# Write Functionality for 4 ingestion @David
# Documentation ---- @David
### Time-Series Data Model ---- @David
# Code review ----- next week @David @Joseph
# List of 'things' to decide ---- @Joseph first

# Incorporate Logging @David