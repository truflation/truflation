"""
Signer for httpd web
"""

import os
from icecream import ic
from abc import ABC, abstractmethod
from jwcrypto import jwk, jwt
from eth_account import Account
from eth_account.messages import encode_typed_data

ic.disable()

# different versions of python seem to have
# different conventions regarding adding 0x
def hexbytes_no0x_str(hexb):
    s = hexb.hex()
    if s.startswith('0x'):
        s = s[2:]
    return s

def convert_floats_to_wei(json_dict):
    def float_to_int(num):
        if isinstance(num, list):
            return [ float_to_int(x) for x in num ]
        if isinstance(num, dict):
            return convert_floats_to_wei(num)
        if isinstance(num, float):
            return int(num * 10**18)
        return num
    return {k: float_to_int(v) for k, v in json_dict.items()}

class Signer(ABC):
    """
    ABC Signer
    """
    def __init__(
            self,
            privkey=None,
            pubkey=None
    ):
        self.privkey = privkey
        self.pubkey = pubkey
    def preprocess(self, payload):
        return payload
    @abstractmethod
    def auth_info(self):
        """
        return auth string
        """
        raise NotImplementedError
    @abstractmethod
    def signature(self, payload, **kwargs):
        """
        return signature
        """
        raise NotImplementedError
    @classmethod
    def factory(
            cls,
            sign_string : str | None,
            privkey=None,
            pubkey=None,
            *args,
            **kwargs
    ):
        if sign_string == "jwt":
            return JwtSigner(
                privkey,
                pubkey,
                *args,
                **kwargs
            )
        if sign_string == "eip712":
            return Eip712Signer(
                privkey,
                pubkey,
                *args,
                **kwargs
            )
        return NullSigner()

class JwtSigner(Signer):
    """
    JWT Signer
    """
    def __init__(self,
                 privkey=None,
                 pubkey=None,
                 *args,
                 **kwargs):
        super().__init__(privkey, pubkey)
        if privkey is None:
            privkey = os.environ.get('RT_PRIV_KEY')
        self.privkey = jwk.JWK.from_pem(privkey.encode('utf-8'))
        if pubkey is None:
            pubkey = os.environ.get('RT_PUB_KEY')
        self.pubkey = jwk.JWK.from_pem(pubkey.encode('utf-8'))
        self.kwargs = kwargs
    def auth_info(self):
        return self.pubkey
    def signature(self, payload, **kwargs):
        token = jwt.JWT(
            header={
                'alg': self.kwargs.get('alg', 'ES256K')
            },
            claims=payload
        )
        token.make_signed_token(self.privkey)
        return token.serialize()

class Eip712Signer(Signer):
    """
    Implement

    https://snakecharmers.ethereum.org/typed-data-message-signing/
    
    see
    
    https://github.com/equilibria-xyz/perennial-v2/blob/v2.2/packages/perennial-oracle/contracts/metaquants/MetaQuantsFactory.sol#L62
    """
    def __init__(
            self,
            privkey=None,
            pubkey=None,
            domain=None,
            msgtypes=None,
            *args,
            **kwargs
    ):
        super().__init__(privkey, pubkey)
        if privkey is None:
            privkey = os.environ.get('ETH_PRIVATE_KEY')
        self.privkey = privkey
        address = \
            Account.from_key(self.privkey).address
        if domain is not None:
            if domain.get('verifyingContract') is None:
                domain['verifyingContract'] = address
            if domain['verifyingContract'] != address:
                raise ValueError('domain and private key do not match')
        self.domain = domain
        self.msgtypes = msgtypes
    def auth_info(self):
        return {
            'types': self.msgtypes,
            'domain': self.domain
        }
    def preprocess(self, payload):
        return convert_floats_to_wei(payload)
    def signature(self, payload, **kwargs):
        sm = Account.sign_typed_data(
            self.privkey, None, None, None, {
                'message': payload
            } | self.auth_info()
        )
        ic(sm)
        return {
            'sig': {
                'hash': hexbytes_no0x_str(sm.messageHash),
                'signature': hexbytes_no0x_str(sm.signature)
            }
        }

class NullSigner(Signer):
    def __init__(self):
        super().__init__(None, None)
    def auth_info(self):
        return {}
    def preprocess(self, payload):
        return payload
    def signature(self, payload, **kwargs):
        return None
