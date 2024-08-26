import unittest
import os
import pandas as pd
from sqlalchemy import text
from truflation.data.connector import ConnectorSql
from truflation.data.exporter import Exporter, ExportDetails
from dotenv import load_dotenv

load_dotenv()

@unittest.skipIf('CONNECTOR' not in os.environ, "skipping test")
class TestMySQLPrimaryKey(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorSql(os.getenv('CONNECTOR'))
        cls.engine = cls.connector.engine

        # Create a test table
        with cls.connector.engine.connect() as connection:
            connection.execute(text("CREATE TABLE IF NOT EXISTS test_table (value DOUBLE, date DATE, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"))
            connection.commit()

    @classmethod
    def tearDownClass(cls):
        # Drop the test table after tests are done
        with cls.engine.connect() as connection:
            connection.execute(text('DROP TABLE IF EXISTS test_table'))
            connection.commit()
        cls.connector.engine.dispose()

    def test_export_with_primary_key(self):
        # Create a sample dataframe
        data = {
            'value': [10.5, 20.7, 30.2],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03']
        }
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])

        export_details = ExportDetails(
            name='test_export',
            connector=self.connector,
            key='test_table',
            replace=True
        )

        # Verify that the primary key constraint exists
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SHOW INDEX FROM test_table WHERE Key_name = 'PRIMARY'")
            ).fetchone()
            self.assertIsNone(result) # the primary key doesn't exists

        # Simulate export to the MySQL database
        exporter = Exporter()
        exporter.export(export_details, df)

        # Verify that the data was inserted correctly
        with self.engine.connect() as connection:
            result = connection.execute(text('SELECT * FROM test_table')).fetchall()
            self.assertEqual(len(result), len(df))
            for row, original_row in zip(result, df.itertuples(index=False)):
                self.assertEqual(row[0], original_row.value)  # Comparing 'value'
                self.assertEqual(row[1], original_row.date.date())  # Comparing 'date'

        # Verify that the primary key constraint is applied
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SHOW INDEX FROM test_table WHERE Key_name = 'PRIMARY'")
            ).fetchone()
            self.assertIsNotNone(result) # the primary key exists so next time the pipeline should ignore the primary key check


    def test_update_table_with_null_created_at(self):
        
        export_details = ExportDetails(
            name='test_update_created_at',
            connector=self.connector,
            key='edu_psu_housing',
            replace=True
        )

        exporter = Exporter()
        exporter.update_null_created_at(export_details)   


if __name__ == '__main__':
    unittest.main()
