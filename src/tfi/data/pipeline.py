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

        # for name, l in self.loaders.items():
        #     print(f'{name}: {l} --> {l.reader}, {l.writer}')

    def ingest(self) -> None:
        # todo -- create try except after functionality works

        # Pre-Ingestion Function
        self.pre_ingestion_function[0]()  # The class saves the function as a tuple

        # Read, Parse,  and Validate from all sources
        print(f'Reading, Parsing, and Validating...')
        for source_name, source in self.sources.items():
            self.loaders[source_name].run(source.source, source_name)
            # my_data = self.loaders[source_name].writer.cache.cache_data["cache"].df
            # my_data = self.loaders[source_name].writer.read_all(key="cache")
            # print(my_data)

        for source_name, loader in self.loaders.items():
            # todo -- i would rather loader work on all types given source -- more abstraction
            # example --- self.loader.run(source, key)
            # This way, I could use one loader class and read all the files and save it into the same place.
            # If i wanted it saved somewhere else--even as a database?--, I would need to specify flags, but the default shoulde be a cache
            # Flag for procoessing on cache with a parser?

            # self.loaders[source_name].run(source.source, source_name)
            pass



        # Transform x sources into y dataframes
        print(f'Transforming ...')
        my_cache = self.loaders[source_name].writer.cache.cache_data
        # todo -- consider incorporating a loader here
        df = self.transformer(my_cache)
        print(f'transformed: {df}')

        #  Export y dataframes into z tables on servers
        print(f'Exporting -- todo')

        #  Post ingestion function
        self.post_ingestion_function[0]()
