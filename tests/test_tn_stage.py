import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from truflation.data.connector.tn_stage import (
    is_us_gated_batch,
    plan_drain,
    read_actionable,
    recover_processing,
    stage_batches,
    stamp_streams,
)


def _batch(stream_id, value, obs='2026-09-27', rewrite=True):
    return {
        'stream_id': stream_id,
        'data_provider': '0xabc',
        'observation_date': obs,
        'rewrite_event_time': rewrite,
        'inputs': [{'value': value}],
    }


class TestTnStage(unittest.TestCase):
    def test_us_gate_is_cpi_family_not_us_named_indexes(self):
        self.assertTrue(is_us_gated_batch('cpi-us_frozen'))
        self.assertTrue(is_us_gated_batch('cpi-divergence-us_live'))
        self.assertTrue(is_us_gated_batch('categories_us_None_frozen'))
        self.assertTrue(is_us_gated_batch('mapping-pce-us_frozen'))
        self.assertTrue(is_us_gated_batch('gov_bea'))
        self.assertFalse(is_us_gated_batch('cpi-uk_frozen'))
        self.assertFalse(is_us_gated_batch('mapping-pce-uk_frozen'))
        self.assertFalse(is_us_gated_batch('gasoline_index'))
        self.assertFalse(is_us_gated_batch('custom_index'))

    def test_stage_omits_event_time_and_drain_keeps_newest(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = stage_batches('cpi-uk_frozen', [_batch('stuk', 1.0)], 'append', root)
            newer = stage_batches('cpi-uk_frozen', [_batch('stuk', 2.5)], 'append', root)
            us = stage_batches('cpi-us_frozen', [_batch('stus', 9.0)], 'append', root)

            stored = json.loads(newer.read_text())
            self.assertEqual(stored['gate'], 'default')
            self.assertNotIn('date', stored['streams'][0]['records'][0])
            self.assertEqual(json.loads(us.read_text())['gate'], 'us')

            default_plan = plan_drain(read_actionable(root), 'default')
            self.assertEqual(default_plan.source_files, [newer])
            self.assertEqual(default_plan.superseded_files, [older])
            self.assertEqual(default_plan.streams[0]['records'][0]['value'], 2.5)
            self.assertTrue(us.is_file())

            us_plan = plan_drain(read_actionable(root), 'us')
            self.assertEqual(us_plan.source_files, [us])
            self.assertEqual(us_plan.superseded_files, [])

            stamped = stamp_streams(default_plan.streams, 1_700_000_000)
            self.assertEqual(stamped[0]['records'][0]['date'], 1_700_000_000)
            self.assertEqual(stamped[0]['records'][0]['value'], 2.5)

    def test_history_dates_are_not_rewritten(self):
        stamped = stamp_streams(
            [{
                'stream_id': 'st1',
                'rewrite_event_time': False,
                'records': [{'date': 100, 'value': 1.0}],
            }],
            999,
        )
        self.assertEqual(stamped[0]['records'][0]['date'], 100)

    def test_recover_processing_returns_files_to_pending(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = stage_batches('custom_index', [_batch('st1', 1)], 'append', root)
            processing = root / 'processing'
            processing.mkdir()
            moved = path.replace(processing / path.name) or processing / path.name
            # Path.replace returns None
            recovered = recover_processing(root)
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0].parent, root / 'pending')
            self.assertFalse(moved.exists())


if __name__ == '__main__':
    unittest.main()
