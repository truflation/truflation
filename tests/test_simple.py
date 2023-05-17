import os
import unittest

from overrides import override
import tfi.data.connector
import tfi.data.data
import tfi.data.task
import tfi.data.validator
import tfi.data.connector

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cache = tfi.data.connector.Cache()

class ReadTask(tfi.data.task.Task):
    def __init__(self,
                 database_: str,
                 filename_: str,
                 table_: str):
        self.filename = filename_
        self.table = table_
        self.reader = tfi.data.connector.ConnectorCsv()
        self.writer = tfi.data.connector.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        with open(
                self.filename,
                newline=''
        ) as csvfile:
            self.writer.drop_table(self.table, True)
            b = self.reader.read_all(csvfile)
            self.writer.write_all(b, table=self.table)

class WriteTask(tfi.data.task.Task):
    def __init__(self,
                 database_: str,
                 filename_: str,
                 table_: str):
        self.filename = filename_
        self.table = table_
        self.writer = tfi.data.writer.ConnectorCsv()
        self.reader = tfi.data.reader.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        with open(
                self.filename, 'w',
                newline=''
        ) as csvfile:
            b = self.reader.read_all(self.table)
            self.writer.write_all(b, csvfile)

class TestSimple(unittest.TestCase):
    def test_simple(self):
        connector = tfi.data.connector.Connector()
        task = tfi.data.task.Task()

    def test_read_csv(self):
        task = ReadTask(
            f'sqlite:///{SCRIPT_DIR}/database.db',
            os.path.join(SCRIPT_DIR, 'eggs.csv'),
            'demo'
        )
        task.run()

    def test_write_csv(self):
        task = WriteTask(
            f'sqlite:///{SCRIPT_DIR}/database.db',
            os.path.join(SCRIPT_DIR, 'eggs.out.csv'),
            'demo'
        )
        task.run()

    def test_cache(self):
        r = tfi.data.connector.ConnectorCache('key1')

if __name__ == '__main__':
    unittest.main()
