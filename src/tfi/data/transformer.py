"""
Transformer
"""

from tfi.data.data import Data

class Transformer:
    def __init__(self):
        pass

    def transform_all(
            self,
            input: Data,
            output: Data,
            *args,
            **kwargs
    ):
        for i in self.transform_chunk(
                input,
                output
        ):
            pass

    def transform_chunk(
            self,
            input: Data,
            output: Data,
            *args,
            **kwargs
    ):
        raise NotImplementedError
