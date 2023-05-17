from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
# from tfi.data.data import DataPandas, DataFormat
from tfi.data.details import PipeLineDetails


class Pipeline(Task):
    # def __init__(self, reader, writer):
    #     super().__init__(reader, writer)
    #     self.data = ["developer_hours", "developer_hours2"]
    #     self.loader = Loader(self.reader, "cache")
    #     self.validator = Validator(
    #         "cache", "cache"
    #     )
    #     self.calculator = \
    #         AddHours("cache", self.writer)

    def __init__(self, pipeline_details: PipeLineDetails):
        # todo -- decide if we are using Task
        # todo -- remove reader and writer and make fluid over all types
        self.reader = "csv"
        self.writer = "csv"
        self.task = Task(self.reader, self.writer)

        # super().__init__(reader, writer)
        self.name = pipeline_details.name
        self.pre_ingestion_function = pipeline_details.pre_ingestion_function,
        self.post_ingestion_function = pipeline_details.post_ingestion_function,
        self.sources = dict({x.name: x for x in pipeline_details.sources})
        self.loader = Loader(self.reader, "cache")  # todo -- this should be general purpose
        self.validator = Validator(self.reader, self.writer)  # todo -- this should be general purpose
        self.transformer = pipeline_details.transformer

        # print(f'ingestion function: {self.pre_ingestion_function}')
        # print(self.pre_ingestion_function())
        print(f'pipeline name: {pipeline_details.name}')

    def ingest(self) -> None:
        # todo -- create try except after functionality works

        # Pre-Ingestion Function
        self.pre_ingestion_function[0]()  # The class saves the function as a tuple

        # Read, Parse,  and Validate from all sources
        for source_name, source in self.sources.items():
            print(f'Reading, Parsing, and Validating {source_name} -> {source.source_type} -> {source.source}')
        #     self.loader.run(source_type, URL)
        #     self.validator.run(URL)

        # Transform x sources into y dataframes
        print(f'Transforming -- todo ')
        self.transformer(self.sources.keys())

        #  Export y dataframes into z tables on servers
        print(f'Exporting -- todo')

        #  Post ingestion function
        self.post_ingestion_function[0]()
