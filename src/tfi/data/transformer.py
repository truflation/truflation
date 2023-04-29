"""
Transformer
"""

from tfi.data.bundle import Bundle

class Transformer:
    def __init__(self):
        pass

    def transform_all(
            self,
            input: Bundle,
            output: Bundle,
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
            input: Bundle,
            output: Bundle,
            *args,
            **kwargs
    ):
        raise NotImplementedError
