import unittest
import pandas as pd
from datetime import datetime, timezone
from truflation.data.exporter import Exporter

class TestRevisionHandling(unittest.TestCase):
    def setUp(self):
        self.exporter = Exporter()
        
    def test_revision_handling(self):
        # Create base dataframe with multiple revisions
        base_data = {
            'date': ['2025-11-05', '2025-11-05', '2025-11-06'],
            'value': [100, 101, 200],
            'created_at': [
                datetime(2025, 11, 5, 10, 0, tzinfo=timezone.utc),  # First revision
                datetime(2025, 11, 5, 11, 0, tzinfo=timezone.utc),  # Second revision
                datetime(2025, 11, 6, 10, 0, tzinfo=timezone.utc)
            ]
        }
        df_base = pd.DataFrame(base_data)
        
        # Test Case 1: New revision with same value as latest
        incoming_data1 = {
            'date': ['2025-11-05'],
            'value': [101],  # Same as latest revision
            'created_at': [datetime(2025, 11, 5, 12, 0, tzinfo=timezone.utc)]
        }
        df_incoming1 = pd.DataFrame(incoming_data1)
        result1 = self.exporter.reconcile_dataframes(df_base, df_incoming1)
        self.assertTrue(result1.empty, "Should skip revision with same value as latest")
        
        # Test Case 2: New revision with different value
        incoming_data2 = {
            'date': ['2025-11-05'],
            'value': [102],  # Different value
            'created_at': [datetime(2025, 11, 5, 12, 0, tzinfo=timezone.utc)]
        }
        df_incoming2 = pd.DataFrame(incoming_data2)
        result2 = self.exporter.reconcile_dataframes(df_base, df_incoming2)
        self.assertFalse(result2.empty, "Should include revision with different value")
        self.assertEqual(result2.iloc[0]['value'], 102)
        
        # Test Case 3: Completely new date
        incoming_data3 = {
            'date': ['2025-11-07'],
            'value': [300],
            'created_at': [datetime(2025, 11, 7, 10, 0, tzinfo=timezone.utc)]
        }
        df_incoming3 = pd.DataFrame(incoming_data3)
        result3 = self.exporter.reconcile_dataframes(df_base, df_incoming3)
        self.assertFalse(result3.empty, "Should include new date")
        self.assertEqual(result3.iloc[0]['value'], 300)

if __name__ == '__main__':
    unittest.main()