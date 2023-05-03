"""
Exporter
"""

from tfi.data.bundle import Bundle, BundleFormat
import pandas
from sqlalchemy import Table, MetaData, create_engine


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
        super(Exporter, self).__init__()
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


    def drop_table(
            self,
            table_name: str
    ):
        tbl = Table(
            table_name, MetaData(),
            autoload_with=self.engine
        )
        tbl.drop(self.engine, checkfirst=False)

class ExporterCSV(Exporter):
    def __init__(self):
        super(Exporter, self).__init__()

    def export_all(
            self,
            data: Bundle,
            *args, **kwargs) -> None:
        data.get(BundleFormat.PANDAS).to_csv(args[0])
