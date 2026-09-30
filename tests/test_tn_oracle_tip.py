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
from truflation.data.export_details import ExportDetails
from truflation.data.exporter import Exporter, _uses_tn_tip, tn_tip_frame


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


def _series():
    """Latest real month plus an older zero the chain diff would leave behind."""
    return pd.DataFrame(
        {
            'date': pd.to_datetime(['2010-03-31', '2026-09-29', '2026-09-29']),
            'value': [0.0, 0.0, 1.5],
            'created_at': pd.to_datetime([
                '2010-04-01 00:00:00',
                '2026-09-29 00:01:00',
                '2026-09-29 12:00:00',
            ]),
        }
    )


class TNConnector:
    """Name matches the writer check. Does not open a chain client."""

    def __init__(self):
        self.reads = 0
        self.writes = []

    def read_all(self, *args, **kwargs):
        self.reads += 1
        raise AssertionError('tip export must not read the chain')

    def write_all(self, data, **kwargs):
        self.writes.append(data.copy())


class SqlConnector:
    def __init__(self, remote):
        self.remote = remote
        self.reads = 0
        self.writes = []

    def read_all(self, *args, **kwargs):
        self.reads += 1
        return self.remote.copy()

    def write_all(self, data, **kwargs):
        self.writes.append(data.copy())


class TestTnTipFrame(unittest.TestCase):
    def test_keeps_newest_date_and_latest_vintage(self):
        tip = tn_tip_frame(_series())
        self.assertEqual(len(tip), 1)
        self.assertEqual(pd.Timestamp(tip['date'].iloc[0]), pd.Timestamp('2026-09-29'))
        self.assertEqual(tip['value'].iloc[0], 1.5)

    def test_date_index(self):
        frame = _series().set_index('date')
        tip = tn_tip_frame(frame)
        self.assertEqual(len(tip), 1)
        self.assertEqual(tip['value'].iloc[0], 1.5)

    def test_empty(self):
        empty = pd.DataFrame(columns=['date', 'value'])
        self.assertTrue(tn_tip_frame(empty).empty)
        self.assertIsNone(tn_tip_frame(None))


class TestUsesTnTip(unittest.TestCase):
    def test_tn_writer(self):
        details = ExportDetails('idx', TNConnector(), 'com_truflation_x')
        self.assertTrue(_uses_tn_tip(details))

    def test_custom_reconcile_stays_on_the_diff(self):
        details = ExportDetails(
            'idx', TNConnector(), 'com_truflation_x', reconcile=lambda remote, local: local,
        )
        self.assertFalse(_uses_tn_tip(details))

    def test_history_mode_stays_on_the_diff(self):
        details = ExportDetails(
            'idx', TNConnector(), 'com_truflation_x', broadcast_history=True,
        )
        self.assertFalse(_uses_tn_tip(details))

    def test_database_writer(self):
        details = ExportDetails('idx', SqlConnector(pd.DataFrame()), 'some_table')
        self.assertFalse(_uses_tn_tip(details))


class TestExportTip(unittest.TestCase):
    def test_tn_export_skips_chain_and_writes_real_latest(self):
        writer = TNConnector()
        details = ExportDetails('idx', writer, 'com_truflation_x')
        written = Exporter().export(details, _series())
        self.assertEqual(writer.reads, 0)
        self.assertEqual(len(writer.writes), 1)
        self.assertEqual(writer.writes[0]['value'].iloc[0], 1.5)
        self.assertEqual(written['value'].iloc[0], 1.5)

    def test_database_export_still_reconciles(self):
        remote = pd.DataFrame(
            {
                'date': pd.to_datetime(['2026-09-29']),
                'value': [1.5],
                'created_at': pd.to_datetime(['2026-09-29 12:00:00']),
            }
        )
        writer = SqlConnector(remote)
        details = ExportDetails('idx', writer, 'some_table')
        written = Exporter().export(details, _series())
        self.assertEqual(writer.reads, 1)
        values = set(written['value'].tolist())
        self.assertIn(0.0, values)
        self.assertNotIn(1.5, values)


if __name__ == '__main__':
    unittest.main()
