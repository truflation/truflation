import os
import unittest
from unittest.mock import patch
from jwcrypto import jwk, jwt
from eth_account import Account
from eth_account.messages import encode_typed_data
from truflation.data.signer import Signer, JwtSigner, Eip712Signer, NullSigner

"""
EC key from
https://techdocs.akamai.com/iot-token-access-control/docs/generate-ecdsa-keys
"""

class TestSigner(unittest.TestCase):
    def setUp(self):
        self.privkey = '0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'
        self.pubkey = '0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'
        self.privkey_pem = """
-----BEGIN EC PRIVATE KEY-----
MHQCAQEEIEYgBlyQVsH7SpHUH7x4RErcckhu7ary/JjhP72Nk19EoAcGBSuBBAAK
oUQDQgAE1MtHIxlGP5TARqBccrddNm1FnYH1Fp+onETz5KbXPSeG5FGwKMUXGfAm
SZJq2gENULFewwymt+9bTXkjBZhh8A==
-----END EC PRIVATE KEY-----
"""
        self.pubkey_pem = """
-----BEGIN PUBLIC KEY-----
MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAE1MtHIxlGP5TARqBccrddNm1FnYH1Fp+o
nETz5KbXPSeG5FGwKMUXGfAmSZJq2gENULFewwymt+9bTXkjBZhh8A==
-----END PUBLIC KEY-----
"""
        self.payload = {'data': 123.456}

    def test_default_setup(self):
        signer = Signer.factory(None)
        self.assertEqual(signer.auth_info(), {})
        self.assertEqual(
            signer.preprocess({'foo': 'bar'}),
            {'foo': 'bar'}
        )
        self.assertEqual(
            signer.signature({'foo': 'bar'}),
            None
        )

    def test_factory(self):
        signer = Signer.factory('jwt', self.privkey_pem, self.pubkey_pem)
        self.assertIsInstance(signer, JwtSigner)
        signer = Signer.factory('eip712', self.privkey, self.pubkey)
        self.assertIsInstance(signer, Eip712Signer)
        signer = Signer.factory('invalid', self.privkey, self.pubkey)
        self.assertIsInstance(signer, NullSigner)

    def test_jwt_signer(self):
        signer = JwtSigner(self.privkey_pem, self.pubkey_pem)
        auth_info = signer.auth_info()
        self.assertEqual(
            auth_info['kid'], "tAt8egTOmgVfprY2yzwD_Pi7BWPT-6HR2-ruSjZVgMs"
        )
        signature = signer.signature(self.payload)
        self.assertEqual(
            signature.split('.')[0:2], ['eyJhbGciOiJFUzI1NksifQ', 'eyJkYXRhIjoxMjMuNDU2fQ']
        )

    def test_eip712_signer(self):
        msgtypes = {'Data': [{'name': 'data', 'type': 'uint256'}]}
        domain = {'name': 'Test', 'version': '1', 'chainId': 1, 'verifyingContract': '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'}
        signer = Eip712Signer(self.privkey, self.pubkey, domain, msgtypes)
        auth_info = signer.auth_info()
        self.assertDictEqual(auth_info, {'types': msgtypes, 'domain': domain})
        preprocessed_payload = signer.preprocess(self.payload)
        self.assertDictEqual(preprocessed_payload, {'data': 123456000000000000000})
        signature = signer.signature(preprocessed_payload)
        self.assertDictEqual(signature, {'sig': {
            'hash': '0xbf319ef75c1b13b026a51eb5f396e6529c2586f9ea782671fb676d41c29266d8',
            'signature': '0x048ab8b5a4398c4f4d5534e280ad973a8713b96166e389994bca8b0daef4db473a7778c378c1649ca4e700d99213102c82c65a324fc91e66e854d022f46165771c'}})

    def test_null_signer(self):
        signer = NullSigner()
        auth_info = signer.auth_info()
        self.assertDictEqual(auth_info, {})
        signature = signer.signature(self.payload)
        self.assertIsNone(signature)

if __name__ == '__main__':
    unittest.main()
