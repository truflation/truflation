import os
import unittest

import tfi.data.reader
import tfi.data.writer
import tfi.data.bundle

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

class TestSimple(unittest.TestCase):
    def test_simple(self):
        bundle = tfi.data.bundle.Bundle()
        reader = tfi.data.reader.Reader()
        writer = tfi.data.writer.Writer()

    def test_read_csv(self):
        with open(
                os.path.join(SCRIPT_DIR, 'eggs.csv'),
                newline=''
        ) as csvfile:
            reader = tfi.data.reader.ReaderCSV()
            writer = tfi.data.writer.WriterSql(
                f'sqlite:///{SCRIPT_DIR}/database.db'
            )
            writer.drop_table('demo', True)
            b = reader.read_all(csvfile)
            writer.write_all(b, table='demo')
"""
    def test_write_csv(self):
        with open(
                os.path.join(
                    SCRIPT_DIR, 'eggs.out.csv'
                ),
                'w',
                newline=''
        ) as csvfile:
            reader = tfi.data.reader.ReaderSql(
                f'sqlite:///{SCRIPT_DIR}/database.db'
            )
            b = reader.read_all('demo')
            writer = tfi.data.writer.WriterCSV()
            writer.write_all(b, csvfile)
"""
if __name__ == '__main__':
    unittest.main()
