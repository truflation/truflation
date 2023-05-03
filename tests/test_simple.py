import os
import unittest

import tfi.data.importer
import tfi.data.exporter
import tfi.data.bundle

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

class TestSimple(unittest.TestCase):
    def test_simple(self):
        bundle = tfi.data.bundle.Bundle()
        importer = tfi.data.importer.Importer()
        exporter = tfi.data.exporter.Exporter()

    def test_read_csv(self):
        with open(
                os.path.join(SCRIPT_DIR, 'eggs.csv'),
                newline=''
        ) as csvfile:
            importer = tfi.data.importer.ImporterCSV()
            exporter = tfi.data.exporter.ExporterSql(
                f'sqlite:///{SCRIPT_DIR}/database.db'
            )
            exporter.drop_table('demo')
            b = importer.import_all(csvfile)
            exporter.export_all(b, table='demo')

    def test_write_csv(self):
        with open(
                os.path.join(
                    SCRIPT_DIR, 'eggs.out.csv'
                ),
                'w',
                newline=''
        ) as csvfile:
            importer = tfi.data.importer.ImporterSql(
                f'sqlite:///{SCRIPT_DIR}/database.db'
            )
            b = importer.import_all('demo')
            exporter = tfi.data.exporter.ExporterCSV()
            exporter.export_all(b, csvfile)

if __name__ == '__main__':
    unittest.main()
