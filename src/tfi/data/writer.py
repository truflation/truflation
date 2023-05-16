"""
Writer
"""

import sqlalchemy
from typing import Iterator, Any
from sqlalchemy import Table, MetaData, create_engine
from tfi.data.data import Data, DataFormat

class Writer:
    def __init__(self):
        pass

    def authenticate(self, token):
        pass

    def write_all(
            self,
            data: Data,
            *args, **kwargs
    ) -> None:
        for i in self.write_chunk(
                data
        ):
            pass

    def write_chunk(
            self,
            data: Data,
            *args, **kwargs
    ) -> Iterator[Any]:
        raise NotImplementedError

class WriterSql(Writer):
    def __init__(self, engine):
        super().__init__()
        self.engine = create_engine(engine)
        self.conn = self.engine.connect()

    def write_all(
            self,
            data: Data,
            *args,
            **kwargs
    ) -> None:
        table = kwargs['table']
        del kwargs['table']
        data.get(DataFormat.PANDAS).to_sql(
            table,
            self.engine,
            **kwargs
        )

    def write_chunk(
            self,
            data: Data,
            *args, **kwargs
    ) -> Iterator[Any]:
        self.write_all(data, *args, **kwargs)
        yield None

    def execute(self, statement, **line):
        self.conn.execute(statement, **line)

    def drop_table(
            self,
            table_name: str,
            ignore_fail: bool = True
    ):
        try:
            tbl = Table(
                table_name, MetaData(),
                autoload_with=self.engine
            )
        except sqlalchemy.exc.NoSuchTableError:
            if ignore_fail:
                return
            raise
        tbl.drop(self.engine, checkfirst=False)

class WriterCsv(Writer):
    def __init__(self):
        super().__init__()

    def write_all(
            self,
            data: Data,
            *args, **kwargs) -> None:
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        data.get(DataFormat.PANDAS).to_csv(filename)
