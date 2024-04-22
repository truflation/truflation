import os
import unittest

import truflation.data.connectors.kwil as kwil
from truflation.data.connector import connector_factory

@unittest.skipIf('KWIL_USER' not in os.environ, "skipping test")
class TestSimple(unittest.TestCase):
    def test_factory(self):
        connector = connector_factory('kwil:')
        self.assertTrue(connector is not None)
        output = connector.ping()
        self.assertTrue(output.strip() == 'pong')

    def test_simple(self):
        connector = kwil.ConnectorKwil()
        output = connector.ping()
        self.assertTrue(output.strip() == 'pong')
        version = connector.version()

    def test_list_databases(self):
        connector = kwil.ConnectorKwil()
        output = connector.list_databases()

