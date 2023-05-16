#!/usr/bin/env python3

import sys
from tfi.data.cache import Cache
from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.loader import Loader
from tfi.data.data import DataPandas, DataFormat
from tfi.data.reader import Reader, ReaderCsv
from tfi.data.writer import WriterCsv


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
        self.cache = Cache()
        self.loader = Loader(reader, self.cache.writer())
        self.validator = Validator(self.cache.reader(), self.cache.writer())
        self.calculator = \
            AddHours(self.cache.reader(), writer)

    def run(self, fileh) -> None:
        for i in self.data:
            self.loader.run(fileh, i)
            self.validator.run(i)
        self.calculator.run()


if __name__ == '__main__':
    r = ReaderCsv()
    p = CalculateDeveloperHours(r, WriterCsv())
    p.run("examples/example.csv")
