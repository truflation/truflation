import os
import unittest

from overrides import override
import tfi.data.connector
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
        self.reader = tfi.data.connector.ConnectorCsv(
            path_root=SCRIPT_DIR
        )
        self.writer = tfi.data.connector.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        self.writer.drop_table(self.table, True)
        b = self.reader.read_all(self.filename)
        self.writer.write_all(b, table=self.table)

class WriteTask(tfi.data.task.Task):
    def __init__(self,
                 database_: str,
                 filename_: str,
                 table_: str):
        self.filename = filename_
        self.table = table_
        self.writer = tfi.data.writer.ConnectorCsv(
            path_root=SCRIPT_DIR
        )
        self.reader = tfi.data.reader.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        b = self.reader.read_all(self.table)
        self.writer.write_all(b, key=self.filename)

class TestSimple(unittest.TestCase):
    def test_simple(self):
        connector = tfi.data.connector.Connector()
        task = tfi.data.task.Task()

    def test_read_csv(self):
        task = ReadTask(
            f'sqlite:///{SCRIPT_DIR}/database.db',
            'eggs.csv',
            'demo'
        )
        task.run()

    def test_write_csv(self):
        task = WriteTask(
            f'sqlite:///{SCRIPT_DIR}/database.db',
            'eggs.out.csv',
            'demo'
        )
        task.run()

    def test_cache(self):
        r = tfi.data.connector.ConnectorCache('key1')

if __name__ == '__main__':
    unittest.main()
