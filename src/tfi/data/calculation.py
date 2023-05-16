"""
Transformer
"""

from tfi.data.data import Data

class Calculation(Task):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def calculate(
            self,
            *args,
            **kwargs
    ):
        pass

    def run(self):
        self.calculate()
