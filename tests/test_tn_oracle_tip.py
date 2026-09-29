import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from truflation.data.connector.trufnetwork import (
    _oracle_tip,
    _oracle_tip_records,
    _should_skip_oracle_tip,
    _values_equal,
    load_tn_watermark,
    save_tn_watermark,
)


class TestOracleTipRecords(unittest.TestCase):
    def test_keeps_latest_value_and_stamps_event_time(self):
        df = pd.DataFrame(
            {
                'date': pd.to_datetime(['2026-09-17', '2026-09-18', '2026-09-19']),
                'value': [1.0, 2.0, 3.5],
            }
        )
        fixed = int(datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc).timestamp())
        records = _oracle_tip_records(df, event_time=fixed)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['date'], fixed)
        self.assertEqual(records[0]['value'], 3.5)
        self.assertEqual(_oracle_tip(df), ('2026-09-19', 3.5))

    def test_empty(self):
        self.assertEqual(_oracle_tip_records(pd.DataFrame(columns=['date', 'value'])), [])
        self.assertIsNone(_oracle_tip(pd.DataFrame(columns=['date', 'value'])))

    def test_values_equal(self):
        self.assertTrue(_values_equal(1.0, 1.0))
        self.assertTrue(_values_equal(1.0, 1.0000000001))
        self.assertFalse(_values_equal(1.0, 1.1))

    def test_publish_when_no_watermark(self):
        self.assertFalse(_should_skip_oracle_tip('2026-09-19', 3.20, None))

    def test_publish_new_observation_day_even_when_flat(self):
        watermark = {'observation_date': '2026-09-19', 'value': 3.20}
        self.assertFalse(_should_skip_oracle_tip('2026-09-20', 3.20, watermark))

    def test_publish_when_same_day_value_changes(self):
        watermark = {'observation_date': '2026-09-19', 'value': 3.20}
        self.assertFalse(_should_skip_oracle_tip('2026-09-19', 3.22, watermark))

    def test_skip_reread_of_same_tip(self):
        watermark = {'observation_date': '2026-09-19', 'value': 3.20}
        self.assertTrue(_should_skip_oracle_tip('2026-09-19', 3.20, watermark))

    def test_watermark_roundtrip(self):
        with TemporaryDirectory() as tmp:
            previous = os.environ.get('TN_WATERMARK_DIR')
            os.environ['TN_WATERMARK_DIR'] = tmp
            try:
                save_tn_watermark('stabc', '2026-09-20', 3.2, '2026-09-21T02:17:00+00:00')
                loaded = load_tn_watermark('stabc')
                self.assertEqual(loaded['observation_date'], '2026-09-20')
                self.assertTrue(_values_equal(loaded['value'], 3.2))
                self.assertTrue(Path(tmp, 'stabc.json').is_file())
                self.assertTrue(_should_skip_oracle_tip('2026-09-20', 3.2, loaded))
                self.assertFalse(_should_skip_oracle_tip('2026-09-21', 3.2, loaded))
            finally:
                if previous is None:
                    os.environ.pop('TN_WATERMARK_DIR', None)
                else:
                    os.environ['TN_WATERMARK_DIR'] = previous


if __name__ == '__main__':
    unittest.main()
