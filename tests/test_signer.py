import unittest
from truflation.data.signer import Signer

class SignerTest(unittest.TestCase):
    def test_export_with_reconciliation(self):
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

if __name__ == '__main__':
    unittest.main()
