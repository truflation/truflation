import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from truflation.data.connector.tn_stage import (
    freeze_event_time,
    group_send_batches,
    plan_drain,
    read_actionable,
    recover_processing,
    split_held,
    stage_batches,
    stamp_streams,
    unpublished_streams,
)


def _batch(stream_id, value, obs='2026-09-27', rewrite=True, table=None):
    return {
        'stream_id': stream_id,
        'table': table,
        'data_provider': '0xabc',
        'observation_date': obs,
        'rewrite_event_time': rewrite,
        'inputs': [{'value': value}],
    }


def _not_us(batch_key, _table):
    return not batch_key.startswith('cpi-us_')


class TestTnStage(unittest.TestCase):
    def test_stage_omits_event_time_and_drain_keeps_newest(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = stage_batches('cpi-uk_frozen', [_batch('stuk', 1.0)], 'append', root)
            newer = stage_batches('cpi-uk_frozen', [_batch('stuk', 2.5)], 'append', root)
            us = stage_batches('cpi-us_frozen', [_batch('stus', 9.0)], 'append', root)

            stored = json.loads(newer.read_text())
            self.assertNotIn('date', stored['streams'][0]['records'][0])

            default_plan = plan_drain(read_actionable(root)[0], _not_us)
            self.assertEqual(default_plan.source_files, [newer])
            self.assertEqual(default_plan.superseded_files, [older])
            self.assertEqual(default_plan.streams[0]['records'][0]['value'], 2.5)
            self.assertTrue(us.is_file())

            us_plan = plan_drain(read_actionable(root)[0], lambda b, t: not _not_us(b, t))
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

    def test_watermark_match_is_not_sent_again(self):
        streams = [
            {'stream_id': 'stuk', 'observation_date': '2026-09-27', 'records': [{'value': 2.5}]},
            {'stream_id': 'stus', 'observation_date': '2026-09-27', 'records': [{'value': 9.0}]},
        ]
        watermarks = {'stuk': {'observation_date': '2026-09-27', 'value': 2.5}}

        def should_skip(obs, value, watermark):
            return bool(watermark) and watermark.get('observation_date') == obs and watermark.get('value') == value

        send, skip = unpublished_streams(streams, watermarks.get, should_skip)
        self.assertEqual([s['stream_id'] for s in skip], ['stuk'])
        self.assertEqual([s['stream_id'] for s in send], ['stus'])

    def test_event_time_is_frozen_after_the_first_stamp(self):
        payload = {
            'streams': [{
                'stream_id': 'stuk',
                'rewrite_event_time': True,
                'records': [{'value': 2.5}],
            }],
        }
        freeze_event_time(payload, 100)
        freeze_event_time(payload, 999)
        self.assertEqual(payload['streams'][0]['records'][0]['date'], 100)
        self.assertFalse(payload['streams'][0]['rewrite_event_time'])

    def test_shared_file_does_not_queue_the_older_copy(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage_batches(
                'cpi-uk_frozen',
                [_batch('uk_index', 100), _batch('uk_yoy', 2.0)],
                'append',
                root,
            )
            stage_batches(
                'cpi-uk_frozen',
                [_batch('uk_yoy', 2.4)],
                'append',
                root,
            )
            payloads, _errors = read_actionable(root)
            plan = plan_drain(payloads, _not_us)
            for path, payload in payloads:
                if path in plan.source_files:
                    freeze_event_time(payload, 1_700_000_000)
            queued = group_send_batches(plan.streams)['append']
            by_stream = {}
            for row in queued:
                by_stream.setdefault(row['stream_id'], []).append(row['inputs'][0]['value'])
            self.assertEqual(by_stream['uk_index'], [100])
            self.assertEqual(by_stream['uk_yoy'], [2.4])
            self.assertEqual({row['inputs'][0]['date'] for row in queued}, {1_700_000_000})

    def test_stage_records_table(self):
        with TemporaryDirectory() as tmp:
            path = stage_batches(
                'custom_index', [_batch('st1', 1, table='com_truflation_eggs_us_index')], 'append', Path(tmp),
            )
            stored = json.loads(path.read_text())
            self.assertEqual(stored['streams'][0]['table'], 'com_truflation_eggs_us_index')

    def test_selector_holds_rejected_streams_in_shared_batch(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = stage_batches(
                'custom_index',
                [
                    _batch('eggs', 1.0, table='com_truflation_eggs_us_index'),
                    _batch('gamefi', 2.0, table='com_truflation_gamefi_index'),
                ],
                'append',
                root,
            )

            def select(_batch_key, table):
                return table != 'com_truflation_eggs_us_index'

            payloads, _errors = read_actionable(root)
            plan = plan_drain(payloads, select)
            self.assertEqual([s['stream_id'] for s in plan.streams], ['gamefi'])
            self.assertEqual([s['stream_id'] for s in plan.held[path]], ['eggs'])

            by_path = dict(payloads)
            held_files = split_held(plan, by_path, root)
            self.assertEqual(len(held_files), 1)
            held = json.loads(held_files[0].read_text())
            self.assertEqual([s['stream_id'] for s in held['streams']], ['eggs'])
            self.assertEqual(held['created_at'], by_path[path]['created_at'])
            original = json.loads(path.read_text())
            self.assertEqual([s['stream_id'] for s in original['streams']], ['gamefi'])

            path.unlink()
            later = plan_drain(read_actionable(root)[0], lambda _b, _t: True)
            self.assertEqual([s['stream_id'] for s in later.streams], ['eggs'])
            self.assertEqual(later.held, {})

    def test_selector_skips_files_with_nothing_selected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = stage_batches('gasoline_index', [_batch('gas', 3.0)], 'append', root)
            plan = plan_drain(read_actionable(root)[0], lambda batch_key, _t: batch_key != 'gasoline_index')
            self.assertEqual(plan.streams, [])
            self.assertEqual(plan.source_files, [])
            self.assertEqual(plan.superseded_files, [])
            self.assertEqual(plan.held, {})
            self.assertTrue(path.is_file())

    def test_invalid_json_is_reported(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            pending = root / 'pending'
            pending.mkdir()
            bad = pending / 'broken.json'
            bad.write_text('{')
            payloads, errors = read_actionable(root)
            self.assertEqual(payloads, [])
            self.assertEqual(errors[0][0], bad)
            self.assertTrue(bad.is_file())


if __name__ == '__main__':
    unittest.main()
