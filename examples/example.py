#!/usr/bin/env python3

from tfi.data.cache import Cache
from tfi.data.ingestor import Ingestor
from tfi.data.task import Task
from tfi.data.reader import Reader, ReaderCsv, ReaderSpecializedCsv
from tfi.data.writer import WriterCsv

class AddHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
    
    def run():
        df1 = self.reader.get(key="developer_hours")
        df2 = self.reader.get(key="developer_hours2")

        res_df = df1.copy()
        res_df["hours coding"] = df1["hours coding"].add(df2["hours coding"])
        self.writer.set(res_df, key="hours_coding")

class CalculateDeveloperHours(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
        self.cache = Cache()
        self.ingestors = [
            Ingestor(reader, self.cache.writer(), "example.csv", "developer_hours"),
            Ingestor(reader, self.cache.writer(), "example.csv", "developer_hours2")
        ]
        self.calculator = \
            AddHours(self.cache.reader(), writer)

    def run(self) -> None:
        for i in self.ingestors:
            i.run()
        self.calculator.run()
    
if __name__ == '__main__':
    r = ReaderSpecializedCsv()
    p = CalculateDeveloperHours(r, WriterCsv())
    p.run()
