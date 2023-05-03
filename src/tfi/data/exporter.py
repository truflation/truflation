"""
Exporter
"""

from tfi.data.bundle import Bundle, BundleFormat
import pandas
from sqlalchemy import create_engine

class Exporter:
    def __init__(self):
        pass

    def export_all(
            self,
            inputb: Bundle,
            *args, **kwargs
    ):
        for i in self.export_chunk(
                inputb
        ):
            pass


    def export_chunk(
            self,
            inputb: Bundle,
            *args, **kwargs
    ):
        raise NotImplementedError

class ExporterSql(Exporter):
    def __init__(self, engine):
        print(engine)
        self.engine = create_engine(engine)
    def export_all(
            self,
            inputb: Bundle,
            *args,
            **kwargs
    ):
        inputb.get(BundleFormat.PANDAS).to_sql(
            kwargs['table'],
            self.engine
        )

    def export_chunk(
            self,
            inputb: Bundle,
            *args, **kwargs
    ):
        self.export_all(inputb, *args, **kwargs)

    
