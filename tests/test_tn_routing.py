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
from trufnetwork_sdk_py.utils import generate_stream_id

OLD = PROVIDERS_MAP['com_truflation']
EGGS = 'stf6584cf470744723c90130130cb7db'  # eggs_us_index, moved
OTHER = 'st1c87578cef8c9adde10c2f0b86c479'  # us_cpi_index, not moved

EPS_NAMES = {
    'st06238eb10b693a7bfa16d73511ddbe': 'truf_eps_7974.t_gaap',
    'stc7b317cc0a5c4442b53297bf1ae316': 'truf_eps_abnb_gaap',
    'st246fb632eb0222315ff8d1f6413904': 'truf_eps_afrm_gaap',
    'stced16425331d41c3bbe830e32d8208': 'truf_eps_amd_nongaap',
    'stf5ca22483fde14fca3e66fe11a7b5a': 'truf_eps_arm_nongaap',
    'stf389ca288ea32310d279f6b8f66f56': 'truf_eps_cost_gaap',
    'st3b20cbb4cacb461ac53a873ca2f245': 'truf_eps_dash_gaap',
    'st980dcc2aa747e20505d7975706ce9d': 'truf_eps_dis_nongaap',
    'stbaf822bf0824b8cf303d4b8299d267': 'truf_eps_duol_gaap',
    'st906b33483913563eb550ff7278e986': 'truf_eps_elf_nongaap',
    'stc8f7e96fdb6ab7001e4917bd1a8bf5': 'truf_eps_hims_gaap',
    'ste38be285ffb9d2167199febed02b8d': 'truf_eps_mrna_gaap',
    'stcfc1ee47859e1ce657ba2a3e4a4225': 'truf_eps_nvda_nongaap',
    'stf6bc6176620a72a4933b2f4b0f88b0': 'truf_eps_orcl_nongaap',
    'stb94cbe1defa7f44dfbdafed4567c76': 'truf_eps_pfe_nongaap',
    'sta76f27aa64645107db2972845a93f6': 'truf_eps_pltr_nongaap',
    'stfee4a059212ce5a1d0e57d3972682f': 'truf_eps_tem_nongaap',
    'st63e16a3ec6169aa8c01469d2b8d4c5': 'truf_eps_vrtx_nongaap',
    'st1701447bdbd48ee8027b5b5acc8c8e': 'truf_eps_wmt_nongaap',
    'stdad62e6bdad57cd4b8962276fed700': 'truf_eps_xyz_nongaap',
}


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
        self.assertEqual(parse_stream_id('com_truflation_us_latest_frozen_yoy_rate', PROVIDERS_MAP)[2], PREDICT_PROVIDER)
        self.assertEqual(parse_stream_id('com_truflation_us_cpi_index', PROVIDERS_MAP)[2], OLD)

    def test_only_streams_of_the_old_wallet_move(self):
        self.assertEqual(parse_stream_id('com_coingecko_eggs_us_index', PROVIDERS_MAP)[2], PROVIDERS_MAP['com_coingecko'])

    def test_eps_streams_move(self):
        self.assertEqual(parse_stream_id('com_truflation_truf_eps_amd_nongaap', PROVIDERS_MAP)[2], PREDICT_PROVIDER)
        self.assertEqual(parse_stream_id('com_truflation_truf_eps_7974.t_gaap', PROVIDERS_MAP)[2], PREDICT_PROVIDER)

    def test_eps_company_added_later_moves_by_prefix(self):
        self.assertEqual(parse_stream_id('com_truflation_truf_eps_newco_gaap', PROVIDERS_MAP)[2], PREDICT_PROVIDER)

    def test_staged_eps_file_resolves_by_id(self):
        # Files staged before the move hold the old provider and the hashed ID only.
        self.assertEqual(stream_provider(generate_stream_id('truf_eps_mrna_gaap'), OLD), PREDICT_PROVIDER)

    def test_eps_prefix_applies_only_to_the_old_wallet(self):
        self.assertEqual(parse_stream_id('com_coingecko_truf_eps_amd_nongaap', PROVIDERS_MAP)[2], PROVIDERS_MAP['com_coingecko'])

    def test_listed_eps_ids_match_their_names(self):
        for stream_id, name in EPS_NAMES.items():
            self.assertIn(stream_id, tn.EPS_STREAMS)
            self.assertEqual(generate_stream_id(name), stream_id)
        self.assertEqual(len(tn.EPS_STREAMS), len(set(tn.EPS_STREAMS)))

    def test_provider_match_ignores_case(self):
        self.assertEqual(stream_provider(EGGS, '0x' + OLD[2:].upper()), PREDICT_PROVIDER)


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

        def make_client(endpoint, key):
            if key not in clients:
                raise RuntimeError(f'error creating signer: invalid byte in {key}')
            return clients[key]

        for p in (
            mock.patch.object(tn, 'TNClient', make_client),
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

    def test_malformed_key_fails_only_its_streams(self):
        with mock.patch.dict(os.environ, {'TN_PRIVATE_KEY_PREDICT': 'not-hex'}), \
                mock.patch.object(self.conn.logging_manager, 'log_error') as log_error:
            status = self.conn.insert_batches([_batch(EGGS), _batch(OTHER)], 'key', 'append')
        self.assertEqual(status, 'failed')
        self.assertEqual(self.old.inserted, [OTHER])
        self.assertEqual(self.new.inserted, [])
        logged = ' '.join(str(c) for c in log_error.call_args_list)
        self.assertIn('TN_PRIVATE_KEY_PREDICT is not a usable key', logged)
        self.assertNotIn('not-hex', logged)

    def test_key_for_another_wallet_is_refused(self):
        self.new.account = '0x' + '1' * 40
        with mock.patch.dict(os.environ, {'TN_PRIVATE_KEY_PREDICT': 'predict-key'}):
            status = self.conn.insert_batches([_batch(EGGS)], 'key', 'append')
        self.assertEqual(status, 'failed')
        self.assertEqual(self.new.inserted, [])


if __name__ == '__main__':
    unittest.main()
