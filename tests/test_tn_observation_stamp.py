import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd

from truflation.data.connector import trufnetwork as tn
from truflation.data.connector.tn_stage import read_actionable, plan_drain, stamp_streams

OCT_2026 = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp())


def _cpi():
    return pd.DataFrame({
        'date': pd.to_datetime(['2026-08-01', '2026-09-01', '2026-10-01']),
        'value': [334.98, 336.94, 337.511],
    })


class TestObservationStamp(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        env = mock.patch.dict(os.environ, {
            'TN_WATERMARK_DIR': str(self.root / 'watermarks'),
            'TN_STAGE_DIR': str(self.root / 'stage'),
        })
        env.start()
        self.addCleanup(env.stop)
        with mock.patch.object(tn, 'TNClient'), mock.patch.object(tn, 'BulkInserter'):
            self.conn = tn.TNConnector(private_key='key', endpoint='http://fake')

    def _write(self, **kwargs):
        self.conn.write_all(_cpi(), key='com_truflation_gov_bls_cpi', batch_key='gov_bls_cpi', **kwargs)
        return self.conn._batch_buffer['gov_bls_cpi'][0]

    def test_live_mode_stamps_the_data_month(self):
        with mock.patch.dict(os.environ, {'TN_WRITE_MODE': 'live'}):
            entry = self._write(stamp_observation_date=True)
        self.assertEqual(entry['inputs'], [{'date': OCT_2026, 'value': 337.511}])
        self.assertFalse(entry['rewrite_event_time'])

    def test_stage_mode_keeps_the_data_month_through_the_drain(self):
        with mock.patch.dict(os.environ, {'TN_WRITE_MODE': 'stage'}):
            self.conn.write_all(_cpi(), key='com_truflation_gov_bls_cpi', batch_key='gov_bls_cpi',
                                stamp_observation_date=True, finalize=True)
        plan = plan_drain(read_actionable(self.root / 'stage')[0], lambda b, t: True)
        streams = stamp_streams(plan.streams, 1_800_000_000)
        self.assertEqual(streams[0]['records'], [{'date': OCT_2026, 'value': 337.511}])

    def test_default_still_stamps_publish_time(self):
        with mock.patch.dict(os.environ, {'TN_WRITE_MODE': 'live'}):
            entry = self._write()
        self.assertGreater(entry['inputs'][0]['date'], OCT_2026 + 30 * 86400)

    def test_default_stage_mode_leaves_event_time_to_the_drain(self):
        with mock.patch.dict(os.environ, {'TN_WRITE_MODE': 'stage'}):
            entry = self._write()
        self.assertNotIn('date', entry['inputs'][0])
        self.assertTrue(entry['rewrite_event_time'])


if __name__ == '__main__':
    unittest.main()
