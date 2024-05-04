#!/usr/bin/env python3
"""
Unit test / debug framework for eip712

see https://snakecharmers.ethereum.org/typed-data-message-signing/
using eth_account calls which are different

This unittest is designed to also make it easy to debug the signers
set change ic.disable to ic.enable() and then set the baselines to
an empty dict and the unittest will dump out a diagnostic of inputs
"""

import unittest
from icecream import ic
from truflation.data.signer import Eip712Signer
from eth_account import Account
from eth_account.messages import encode_typed_data

ic.disable()
#ic.enable()

class TestEip712(unittest.TestCase):
    def setUp(self):
        # Set this to an empty dict to disable comparison
        self.baselines = {
            'preprocess': {'c': [5594541000000000000,
                                 5593205000000000000,
                                 5593320000000000000,
                                 5592133000000000000,
                                 5592080000000000000],
                           's': 'ok',
                           't': [1714817740000,
                                 1714817817000,
                                 1714817878000,
                                 1714817956000,
                                 1714818041000]},
            'signature': {'sig': {
                'hash': 'e3b77601cd9f96af0bdd7ee836e4f51906158c1fa5a1efeae9367d4bc62ce308',
                'signature': 'd27ffd8f614611954df3269f7346edf3b2842c97728167666ea9eef14db201e91792ee271651cb8a52d6edb604753ed7dd734a76bc10dcb7403e0518dfd888a31c'}}
        }

        self.auth_info = {
            "types":{
                "History":[{"name":"s","type":"string"},
                           {"name":"t","type":"uint64[]"},
                           {"name":"c","type":"int256[]"}]},
            "domain":{
                "name":"Truflation test","version":"1","chainId":1,
                "verifyingContract":"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                "salt":"decafbeef"
            }}
        self.message = {
            "s":"ok",
            "t":[1714817740000,1714817817000,1714817878000,1714817956000,1714818041000],
            "c":[5.594541,5.593205,5.59332,
                 5.592133,5.59208]
        }
        self.private_key = \
            '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
        self.signer = Eip712Signer(
            self.private_key,
            None,
            self.auth_info['domain'],
            self.auth_info['types']
        )
        self.address = \
            Account.from_key(self.private_key).address

        # convert floats to wei
        self.payload = ic(
            self.signer.preprocess(self.message)
        )

        self.signature = ic(
            self.signer.signature(self.payload)
        )

        self.signable_msg = ic(
            encode_typed_data(
                self.auth_info['domain'],
                self.auth_info['types'],
                self.payload
            )
        )
    def test_domain_contract(self):
        domain = {
            "name":"Truflation test","version":"1","chainId":1,
            "salt":"decafbeef"
        }
        signer = Eip712Signer(
            self.private_key,
            None,
            domain,
            self.auth_info['types']
        )
        self.assertEqual(
            signer.auth_info()['domain'].get('verifyingContract'),
            self.address
        )

    def test_domain_mismatch(self):
        with self.assertRaises(ValueError) as context:
            domain = {
                "name":"Truflation test","version":"1","chainId":1,
                "verifyingContract":"0xbadbadbadaad88F6F4ce6aB8827279cffFb92266",
                "salt":"decafbeef"
            }
            Eip712Signer(
                self.private_key,
                None,
                domain,
                self.auth_info['types']
            )

    def test_preprocess(self):
        if self.baselines.get('preprocess') is not None:
            self.assertEqual(self.baselines['preprocess'], self.payload)

    def test_signature(self):
        if self.baselines.get('signature') is not None:
            self.assertEqual(self.baselines['signature'], self.signature)

    def test_recover_message(self):
        calling_contract = ic(Account.recover_message(
            self.signable_msg, None, bytes.fromhex(
                self.signature['sig']['signature']
            )
        ))
        self.assertEqual(calling_contract, self.address)

if __name__ == '__main__':
    unittest.main()
