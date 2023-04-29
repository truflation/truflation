"""
Exporter
"""

from tfi.data.bundle import Bundle

class Exporter:
    def __init__(self):
        pass

    def export_all(
            self,
            input: Bundle,
            *args, **kwargs
    ):
        for i in self.export_chunk(
                input
        ):
            pass


    def export_chunk(
            self,
            input: Bundle,
            *args, **kwargs
    ):
        raise NotImplementedError
