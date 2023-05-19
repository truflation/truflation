from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
# from tfi.data.data import DataPandas, DataFormat
from tfi.data.pipeline_details import PipeLineDetails
from tfi.data.connector import connector_factory

# todo -- later, create a new pipeline to process al data in parallel -- new class


class Pipeline(Task):
    def __init__(self, pipeline_details: PipeLineDetails):
        # todo -- decide if we are using Task as inherited
        # todo -- remove reader and writer and make fluid over all types

        # todo -- this is going to be removed ----  vvvvv
        self.reader = "csv"
        self.writer = "csv"
        self.task = Task(self.reader, self.writer)
        # todo -- remove ----  ^^^^^^^^^

        # super().__init__(reader, writer)
        self.name = pipeline_details.name
        self.pre_ingestion_function = pipeline_details.pre_ingestion_function,
        self.post_ingestion_function = pipeline_details.post_ingestion_function,
        self.sources = dict({x.name: x for x in pipeline_details.sources})
        # self.readers = dict({x.name: connector_factory(f'{x.source_type}:{x.source}') for x in pipeline_details.sources})

        self.loaders = dict({name: Loader(source.source_type, "cache") for (name, source) in self.sources.items()}) #         Loader(self.reader, "cache")  # todo -- this should be general purpose
        self.validator = Validator(self.reader, self.writer)  # todo -- this should be general purpose
        self.transformer = pipeline_details.transformer

        for name, l in self.loaders.items():
            print(f'{name}: {l} --> {l.reader}, {l.writer}')

    def ingest(self) -> None:
        # todo -- create try except after functionality works

        # Pre-Ingestion Function
        self.pre_ingestion_function[0]()  # The class saves the function as a tuple


        # self.loader = Loader(self.reader, "cache")
        # self.validator = Validator(
        #     "cache", "cache", constraints="json:data"
        # )
        # self.calculator = \
        #     AddHours("cache", self.writer)


        '''
        Dev note --
        
        It is not straightforward to implement the loaders. I feel like it would be less code heavy and more inuitive
        to have generalized readers that return dataframes.
        
        '''
        # for name, reader in self.readers.items():
        #     res = reader.read_all()
        #     print(res)

        '''
        Dev Note -- I don't understand why these loops are looking at duplicated dat, except A
        '''

        print("\n\nA------------------")
        # Read, Parse,  and Validate from all sources
        for source_name, source in self.sources.items():
            print(f'Reading, Parsing, and Validating {source_name} -> {source.source_type} -> {source.source}')
            self.loaders[source_name].run(source.source, "cache")
            # my_data = self.loaders[source_name].writer.cache.cache_data["cache"].df
            my_data = self.loaders[source_name].writer.read_all(key="cache")
            print(my_data)


        print("\n\nB------------------")
        for source_name, source in self.sources.items():
            print(f'Reading, Parsing, and Validating {source_name} -> {source.source_type} -> {source.source}')
            self.loaders[source_name].run(source.source, "cache")
            # my_data = self.loaders[source_name].writer.cache.cache_data["cache"].df
            my_data = self.loaders[source_name].writer.read_all(key="cache")
            print(my_data)


        print("\n\nC------------------")
        for source_name, loader in self.loaders.items():
            print(f'{source_name} -> {loader}')
            my_data = loader.writer.read_all(key="cache")
            print(my_data)


        print("\n\nD------------------")
        for source_name, source in self.sources.items():
            print(f'{source_name} -> {source}')
            my_data = self.loaders[source_name].writer.read_all(key="cache")
            print(my_data)


        # Transform x sources into y dataframes
        print(f'Transforming -- todo ')
        my_data = dict({name: loader.writer.read_all(key="cache") for (name, loader) in self.loaders.items()})

        # print(f'my_data: {my_data}')
        self.transformer(self.sources.keys())

        #  Export y dataframes into z tables on servers
        print(f'Exporting -- todo')

        #  Post ingestion function
        self.post_ingestion_function[0]()
