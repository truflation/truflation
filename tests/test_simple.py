import unittest

import tfi.data.importer
import tfi.data.exporter
import tfi.data.bundle

class TestSimple(unittest.TestCase):
    def test_simple(self):
        bundle = tfi.data.bundle.Bundle()

if __name__ == '__main__':
    unittest.main()
