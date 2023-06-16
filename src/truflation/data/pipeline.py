import datetime

from truflation.data.validator import Validator
from truflation.data.task import Task
from truflation.data.loader import Loader
from truflation.data.general_loader import GeneralLoader
# from truflation.data.data import DataPandas, DataFormat
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.exporter import Exporter
from typing import Dict


class Pipeline(Task):
    """
    A class that defines a data pipeline, used for ingesting, transforming, and exporting data.

    This class inherits from the `Task` base class and implements the ingestion
    process for a specific pipeline, as defined by a `PipeLineDetails` object.
    Each `Pipeline` object can ingest data from multiple sources, transform the
    data, and then export it to multiple destinations.

    Attributes
    ----------
    name : str
        The name of the pipeline.
    pre_ingestion_function : callable
        The function to run before the ingestion process starts.
    post_ingestion_function : callable
        The function to run after the ingestion process ends.
    sources : dict
        The sources where the pipeline should ingest data from.
    loader : GeneralLoader
        The loader to use for ingesting and parsing the data.
    transformer : Transformer
        The transformer to use for transforming the data.
    exports : list
        A list of exports where the pipeline should send the data.
    exporter : Exporter
        The exporter to use for sending the data.

    Methods
    -------
    ingest() -> None:
        Executes the entire pipeline: ingestion, transformation, and exporting.
    header(s: str) -> None:
        Prints a header for a section of the pipeline process.
    """
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

    def ingest(self, dry_run=False) -> None | Dict:
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
        exports = dict() if not dry_run else \
            dict({export_details.name: my_cache.get(export_details.name, None) for export_details in self.exports})

        #  Export y dataframes into z tables on servers
        self.header("Exporting...")
        for export_details in self.exports:
            exports[export_details.name + 'reconciled_export'], exports[export_details.name + 'reconciled_export'] = \
                self.exporter.export(export_details, my_cache.get(export_details.name, None), dry_run)

        #  Post ingestion function
        self.header("Post Ingestion Function...")
        self.post_ingestion_function[0]()

        if dry_run:
            return {
                "my_cache": my_cache,
                "exports": exports
            }

        """
        self.header("TESTING FROZEN DATA...")
        df_frozen = self.exporter.get_frozen_data(self.exports[0], frozen_datetime=datetime.datetime.now() - datetime.timedelta(days=5)) # Timestamp for may 19 2023
        print(f'df_frozen: \n{df_frozen}')
        """
        
    @staticmethod
    def header(s: str):
        print('\n' + f'#'*20 + f'   {s}   ' + '#'*(20 - len(s)))
