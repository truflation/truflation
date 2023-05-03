"""
Importer
"""

import csv
import pandas
from typing import Optional
from tfi.data.bundle import Bundle, BundlePandas

class Importer:
    """
    Base class for Import
    """
    def __init__(self):
        pass

    def import_all(
            self,
            *args,
            **kwargs
    ) -> Optional[Bundle]:
        bundle = None
        while True:
            b: Optional[Bundle] = self.import_chunk(b)
            if b is None:
                break
            bundle = b
        return bundle

    def import_chunk(
            self,
            outputb: Optional[Bundle],
            *args,
            **kwargs
    ) -> Optional[Bundle]:
        return None

class ImporterCSV(Importer):
    def __init__(self):
        super(Importer, self).__init__()

    def import_all(
            self,
            *args, **kwargs) -> Optional[Bundle]:
        df = pandas.read_csv(args[0])
        return BundlePandas(df)
