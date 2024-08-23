#!/usr/bin/env python3
"""
Unit test / debug framework for Exporter class.

This unittest is designed to test the `export` method of the `Exporter` class
by connecting to a real database and verifying that data is correctly inserted
with the primary key and auto-incremented ID.
"""

import os
import unittest
import pandas as pd
import datetime

from sqlalchemy import create_engine
from truflation.data.export_details import ExportDetails
from truflation.data.exporter import Exporter


@unittest.skipIf('DB_USER' not in os.environ, "skipping test")
class TestExporter(unittest.TestCase):
    def setUp(self):
        # Setup real database connection details
        self.db_details = ExportDetails(
            username=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            host=os.environ.get('DB_HOST', "127.0.0.1"),
            port=os.environ.get('DB_PORT', "3306"),
            db=os.environ.get('DB_NAME', "test_db"),
            table=os.environ.get('DB_TABLE', "test_table")
        )
        # Setup the test data
        self.df_local = pd.DataFrame({
            'date': [datetime.datetime(2023, 8, 21)],
            'value': [100.0],
            'created_at': [datetime.datetime.utcnow()]
        })

        # Create an Exporter instance
        self.exporter = Exporter()

        # Connect to the database
        sql_alchemy_uri = f'mariadb+pymysql://{self.db_details.username}:{self.db_details.password}@{self.db_details.host}:{self.db_details.port}/{self.db_details.db}'
        self.engine = create_engine(sql_alchemy_uri)

        # Ensure the test table exists
        with self.engine.connect() as connection:
            connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.db_details.table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    value FLOAT,
                    created_at DATETIME
                );
            """)

    def tearDown(self):
        # Clean up the test table after each test
        with self.engine.connect() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {self.db_details.table}")

    def test_export(self):
        # Test exporting the data
        result = self.exporter.export(self.db_details, self.df_local, dry_run=False)

        # Verify the data was inserted correctly
        with self.engine.connect() as connection:
            result = connection.execute(f"SELECT * FROM {self.db_details.table}")
            rows = result.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['value'], 100.0)

    def test_primary_key(self):
        # Ensure primary key and auto-incremented ID are in place
        with self.engine.connect() as connection:
            result = connection.execute(f"""
                SHOW KEYS FROM {self.db_details.table}
                WHERE Key_name = 'PRIMARY';
            """)
            keys = result.fetchall()
            self.assertTrue(any(key['Column_name'] == 'id' for key in keys))

if __name__ == '__main__':
    unittest.main()
