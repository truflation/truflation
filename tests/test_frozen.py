import unittest
import pandas as pd
from datetime import datetime
from typing import Optional

import dateparser
from truflation.data.frozen import get_frozen_values


class TestGetFrozenValues(unittest.TestCase):
    def test_regression(self):
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02'],
            'created_at': ['2023-01-01 10:00:00', '2023-01-01 11:00:00', '2023-01-02 10:00:00', '2023-01-02 11:00:00'],
            'key1': ['A', 'A', 'B', 'B'],
            'key2': ['X', 'Y', 'X', 'Y'],
            'value': [1, 2, 3, 4]
        })
        keys = ['key1', 'key2']
        frozen_date = '2023-01-02 10:30:00'
        expected_result = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02'],
            'created_at': ['2023-01-01 10:00:00', '2023-01-01 11:00:00', '2023-01-02 10:00:00', '2023-01-02 11:00:00'],
            'key1': ['A', 'A', 'B', 'B'],
            'key2': ['X', 'Y', 'X', 'Y'],
            'value': [1, 2, 3, 4]
        })
        result = get_frozen_values(df, keys, frozen_date)
        print(result)
        print(expected_result)
        self.assertDictEqual(result.to_dict(), expected_result.to_dict())

                        
