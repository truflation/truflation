#!/usr/bin/env python3

from tfi.data.cache import Cache
from tfi.data.validator import Validator
from tfi.data.task import Task
from tfi.data.reader import Reader, ReaderCsv, ReaderSpecializedCsv
from tfi.data.writer import WriterCsv

class Loader(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
    def run(self, source, key):
        pass

class AddHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
    
    def run(self):
        df1 = self.reader.read_all(key="developer_hours")
        df2 = self.reader.read_all(key="developer_hours2")

        res_df = df1.copy()
        res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
        self.writer.set(res_df, key="hours_coding")

class CalculateDeveloperHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
        self.data = ["developer_hours", "developer_hours2"]
        self.cache = Cache()
        self.loader = Loader(reader, self.cache.writer())
        self.validator = Validator(self.cache.reader(), self.cache.writer())
        self.calculator = \
            AddHours(self.cache.reader(), writer)

    def run(self) -> None:
        for i in self.data:
            self.loader.run("example.csv", i)
            self.validator.run(i)
        self.calculator.run()
    
if __name__ == '__main__':
    r = ReaderSpecializedCsv()
    p = CalculateDeveloperHours(r, WriterCsv())
    p.run()
