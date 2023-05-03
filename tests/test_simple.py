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
            b = importer.import_all(csvfile)
            exporter.export_all(b, table='demo')

if __name__ == '__main__':
    unittest.main()
