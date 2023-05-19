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

        self.header("Cache:...")
        print(f'{self.loader.cache}')

        # Transform x sources into y dataframes
        self.header("Transforming...")
        self.loader.transform(self.transformer)

        # todo -- remove -- print only to show success
        my_cache = self.loader.cache
        print(f'my_cache: {my_cache}')

        #  Export y dataframes into z tables on servers
        self.header("Exporting...")
        for export_details in self.exports:
            self.exporter.export(export_details, my_cache.get(export_details.name, None))

        #  Post ingestion function
        self.header("Post Ingestion Function...")
        self.post_ingestion_function[0]()

    @staticmethod
    def header(s: str):
        print('\n\n' + f'#'*20 + f'   {s}   ' + '#'*(20 - len(s)))
