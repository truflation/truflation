import os
import unittest

from truflation.data.connector import connector_factory, ConnectorKwil

@unittest.skipIf('KWIL_USER' not in os.environ, "skipping test")
class TestSimple(unittest.TestCase):
    def test_factory(self):
        connector = connector_factory('kwil:')
        self.assertTrue(connector is not None)
        output = connector.ping()
        self.assertTrue(output.strip() == 'pong')

    def test_simple(self):
        connector = ConnectorKwil()
        output = connector.ping()
        self.assertTrue(output.strip() == 'pong')
        version = connector.version()

    def test_list_databases(self):
        connector = ConnectorKwil()
        output = connector.list_databases()

