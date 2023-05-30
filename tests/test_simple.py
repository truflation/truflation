import os
import unittest

from overrides import override
import truflation.data.connector
import truflation.data.task
import truflation.data.validator
import truflation.data.connector
import truflation.data.metadata

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cache = truflation.data.connector.Cache()

class ReadTask(truflation.data.task.Task):
    def __init__(self,
                 database_: str,
                 filename_: str,
                 table_: str):
        self.filename = filename_
        self.table = table_
        self.reader = truflation.data.connector.ConnectorCsv(
            path_root=SCRIPT_DIR
        )
        self.writer = truflation.data.connector.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        self.writer.drop_table(self.table, True)
        b = self.reader.read_all(self.filename)
        self.writer.write_all(b, table=self.table)

class WriteTask(truflation.data.task.Task):
    def __init__(self,
                 database_: str,
                 filename_: str,
                 table_: str):
        self.filename = filename_
        self.table = table_
        self.writer = truflation.data.writer.ConnectorCsv(
            path_root=SCRIPT_DIR
        )
        self.reader = truflation.data.reader.ConnectorSql(
            database_
        )

    @override
    def run(self, *args, **kwargs):
        b = self.reader.read_all(self.table)
        self.writer.write_all(b, key=self.filename)

class TestSimple(unittest.TestCase):
    def test_simple(self):
        connector = truflation.data.connector.Connector()
        task = truflation.data.task.Task()

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
        r = truflation.data.connector.ConnectorCache('key1')

class TestMetadataWrite(unittest.TestCase):
    def test_metadata_write(self):
        metadata = truflation.data.metadata.Metadata(
            f'sqlite:///{SCRIPT_DIR}/database.db',
        )

        metadata.write_all('table', {
            'foo': "3434",
            'bar': 232,
            'pow': 1.55
        })

class TestMetadataRead(unittest.TestCase):
    def test_metadata_read(self):
        metadata = truflation.data.metadata.Metadata(
            f'sqlite:///{SCRIPT_DIR}/database.db',
        )
        obj = metadata.read_all('table')
        self.assertEqual(obj['foo'], "3434")

if __name__ == '__main__':
    unittest.main()
