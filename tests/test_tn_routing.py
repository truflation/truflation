import os
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from truflation.data.connector import trufnetwork as tn
from truflation.data.connector.trufnetwork import (
    PREDICT_PROVIDER,
    PROVIDERS_MAP,
    TNConnector,
    parse_stream_id,
    stream_provider,
)

OLD = PROVIDERS_MAP['com_truflation']
EGGS = 'stf6584cf470744723c90130130cb7db'  # eggs_us_index, moved
OTHER = 'st1c87578cef8c9adde10c2f0b86c479'  # us_cpi_index, not moved


class FakeClient:
    def __init__(self, account):
        self.account = account
        self.inserted = []

    def get_current_account(self):
        return self.account

    def batch_stream_exists(self, infos):
        return [{'stream_id': i['stream_id'], 'data_provider': i['data_provider'], 'exists': True} for i in infos]


class FakeInserter:
    def __init__(self, client):
        self.client = client

    def insert_all(self, batches):
        self.client.inserted.extend(b['stream_id'] for b in batches)
        return ['0xtx']


def _batch(stream_id, provider=OLD):
    return {
        'stream_id': stream_id,
        'data_provider': provider,
        'observation_date': '2026-11-10',
        'inputs': [{'date': 1, 'value': 1.0}],
    }


class TestStreamProvider(unittest.TestCase):
    def test_moved_stream_resolves_to_new_wallet(self):
        self.assertEqual(parse_stream_id('com_truflation_eggs_us_index', PROVIDERS_MAP)[2], PREDICT_PROVIDER)
        self.assertEqual(parse_stream_id('com_truflation_us_cpi_index', PROVIDERS_MAP)[2], OLD)

    def test_only_streams_of_the_old_wallet_move(self):
        self.assertEqual(parse_stream_id('com_coingecko_eggs_us_index', PROVIDERS_MAP)[2], PROVIDERS_MAP['com_coingecko'])
        self.assertEqual(stream_provider(EGGS, OLD.upper().replace('0X', '0x')), PREDICT_PROVIDER)


class TestInsertRouting(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patches = [
            mock.patch.dict(os.environ, {'TN_WATERMARK_DIR': tmp.name}),
            mock.patch.object(tn.time, 'sleep'),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.old = FakeClient(OLD)
        self.new = FakeClient(PREDICT_PROVIDER)
        clients = {'old-key': self.old, 'predict-key': self.new}
        for p in (
            mock.patch.object(tn, 'TNClient', lambda endpoint, key: clients[key]),
            mock.patch.object(tn, 'BulkInserter', lambda client, size: FakeInserter(client)),
        ):
            p.start()
            self.addCleanup(p.stop)
        self.conn = TNConnector(private_key='old-key', endpoint='http://fake')

    def test_moved_streams_signed_by_new_key(self):
        with mock.patch.dict(os.environ, {'TN_PRIVATE_KEY_PREDICT': 'predict-key'}):
            # A file staged before the move still names the old wallet.
            status = self.conn.insert_batches([_batch(EGGS), _batch(OTHER)], 'key', 'append')
        self.assertEqual(status, 'ok')
        self.assertEqual(self.new.inserted, [EGGS])
        self.assertEqual(self.old.inserted, [OTHER])

    def test_missing_key_never_falls_back_to_old_key(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TN_PRIVATE_KEY_PREDICT', None)
            status = self.conn.insert_batches([_batch(EGGS), _batch(OTHER)], 'key', 'append')
        self.assertEqual(status, 'failed')
        self.assertEqual(self.old.inserted, [OTHER])
        self.assertEqual(self.new.inserted, [])

    def test_key_for_another_wallet_is_refused(self):
        self.new.account = '0x' + '1' * 40
        with mock.patch.dict(os.environ, {'TN_PRIVATE_KEY_PREDICT': 'predict-key'}):
            status = self.conn.insert_batches([_batch(EGGS)], 'key', 'append')
        self.assertEqual(status, 'failed')
        self.assertEqual(self.new.inserted, [])


if __name__ == '__main__':
    unittest.main()
