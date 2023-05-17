import sys
from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.data import DataPandas, DataFormat
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
        super().__init__(reader, writer)
        self.name = pipeline_details.name
        self.pre_ingestion_function = pipeline_details.pre_ingestion_function,
        self.post_ingestion_function = pipeline_details.post_ingestion_function,
        self.data = pipeline_details.data
        self.loader = Loader(self.reader, "cache")
        self.validator = pipeline_details.validator
        self.calculator =  pipeline_details.calculator


    def run(self, fileh) -> None:
        for i in self.data:
            self.loader.run(fileh, i)
            self.validator.run(i)
        self.calculator.run()
