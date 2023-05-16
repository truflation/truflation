"""
Transformer
"""

from tfi.data.data import Data

class Transformer:
    def __init__(self):
        pass

    def transform_all(
            self,
            input_: Data,
            *args,
            **kwargs
    ) -> Data:
        raise NotImplementedError

    def transform_chunk(
            self,
            input_: Data,
            output_: Data,
            *args,
            **kwargs
    ):
        raise NotImplementedError
