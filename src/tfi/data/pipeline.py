from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.general_loader import GeneralLoader
# from tfi.data.data import DataPandas, DataFormat
from tfi.data.pipeline_details import PipeLineDetails

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


        # todo -- This should be self.loader, loading in a general purpose reader/writer which can function across all
        self.loaders = dict({name: Loader(source.source_type, "cache") for (name, source) in self.sources.items()}) #         Loader(self.reader, "cache")  # todo -- this should be general purpose
        self.loader = GeneralLoader()
        self.validator = Validator(self.reader, self.writer)  # todo -- this should be general purpose
        self.transformer = pipeline_details.transformer

        # for name, l in self.loaders.items():
        #     print(f'{name}: {l} --> {l.reader}, {l.writer}')

    def ingest(self) -> None:
        # todo -- create try except after functionality works
        self.header("Ingesting...")
        # Pre-Ingestion Function

        self.header("Pre Ingestion Function...")
        self.pre_ingestion_function[0]()  # The class saves the function as a tuple

        # Read, Parse,  and Validate from all sources
        self.header("Loading...")
        '''
        # David Replaced this with GeneralizedLoader
        for source_name, source in self.sources.items():
            self.loaders[source_name].run(source.source, source_name)
            # my_data = self.loaders[source_name].writer.cache.cache_data["cache"].df
            # my_data = self.loaders[source_name].writer.read_all(key="cache")
            # print(my_data)
        '''

        for source_name, source_details in self.sources.items():
            self.loader.run(source_details, source_name)

        self.header("Cache:...")
        print(f'{self.loader.cache}')

        # Transform x sources into y dataframes
        self.header("Transforming...")
        # using loader
        # my_cache = self.loaders[source_name].writer.cache.cache_data
        # using generalizedloader

        # todo -- consider incorporating a loader here
        # df = self.transformer(my_cache)
        # self.header("Transformed: ")
        # print(f'{df}')

        self.loader.transform(self.transformer)
        my_cache = self.loader.cache
        print(f'my_cache: {my_cache}')

        #  Export y dataframes into z tables on servers
        self.header("Exporting...")

        #  Post ingestion function
        self.header("Post Ingestion Function...")
        self.post_ingestion_function[0]()

    @staticmethod
    def header(s: str):
        print('\n\n' + f'#'*20 + f'   {s}   ' + '#'*(20 - len(s)))
