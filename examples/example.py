#!/usr/bin/env python3


# My concerns -- is this smart enough to have n data sources that split into y expors via z calculators
# -- How readily will this scale and what will that solution look like when we have multiple of these reading and exporting, especially if there are needed updates
# -- what if I wanted to add preprocess and post process -- after deploying 80 ingestors

# Cameron wants exports to be one time series of numbers: date / value / created_at
# -- one ingestor should create one table (updateable) -- maybe initial setup is, like creating infrastructure tables, can bypass this rule
# --


import sys
from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.data import DataPandas, DataFormat

class AddHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
    
    def run(self):
        df1 = self.reader.read_all(
            key="developer_hours"
        ).get(DataFormat.PANDAS)
        df2 = self.reader.read_all(
            key="developer_hours2"
        ).get(DataFormat.PANDAS)
        res_df = df1.copy()
        res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
        self.writer.write_all(DataPandas(res_df), key="hours_coding")


class CalculateDeveloperHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
        self.data = ["developer_hours", "developer_hours2"]

        self.loader = Loader(self.reader, "cache")
        self.validator = Validator(
            "cache", "cache"
        )
        self.calculator = \
            AddHours("cache", self.writer)

    def run(self, fileh) -> None:
        for i in self.data:
            self.loader.run(fileh, i)
            self.validator.run(i)
        self.calculator.run()


if __name__ == '__main__':
    p = CalculateDeveloperHours("csv", "csv")
    p.run("examples/example.csv")
