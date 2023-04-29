"""
Importer
"""

from tfi.data.bundle import Bundle

class Importer:
    def __init__(self):
        pass

    def import_all(
            self,
            output: Bundle,
            *args,
            **kwargs
    ):
        for i in self.import_chunk(
                output
        ):
            pass

    def import_chunk(
            self,
            output: Bundle,
            *args,
            **kwargs
    ):
        raise NotImplementedError
