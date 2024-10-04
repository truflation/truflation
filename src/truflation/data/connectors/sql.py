
import pandas as pd

from typing import Any, Iterator, List, Optional
from truflation.data.connectors.base import Connector

import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, MetaData


class ConnectorSql(Connector):
    engines = {}

    def __init__(self, engine):
        super().__init__()
        self.engine = self.engines.get(engine)
        if self.engine is None:
            self.engine = \
                create_engine(engine, pool_pre_ping=True)
            self.engines[engine] = self.engine

    # rollbacks are necessary to prevent timeouts
    # see https://stackoverflow.com/questions/58378708/sqlalchemy-cant-reconnect-until-invalid-transaction-is-rolled-back
    # with error Can't reconnect until invalid transaction is rolled back.  Please rollback() fully before proceeding (Background on this error at: https://sqlalche.me/e/20/8s2b)
    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        self.logging_manager.log_debug('Executing SQL query...')
        
        with self.engine.connect() as conn:
            try:
                result_df = pd.read_sql(args[0], conn, dtype_backend='pyarrow', **kwargs)
                self.logging_manager.log_debug('SQL query executed successfully.')
                return result_df
            except Exception as e:
                self.logging_manager.log_debug(f'Error executing SQL query: {e}')
                conn.rollback()
                return None

    def write_all(
            self,
            data,
            *args,
            **kwargs
    ) -> None:
        self.logging_manager.log_info('Saving data to SQL database...')
        
        table = kwargs.pop('key', kwargs.pop('table', None))
        if table is None and len(args) > 0:
            table = args[0]

        with self.engine.connect() as conn:
            try:
                data.to_sql(
                    table,
                    conn,
                    **kwargs
                )
                self.logging_manager.log_info('Data saved to SQL database successfully.')
            except Exception as e:
                self.logging_manager.log_exception(f'Error saving data to SQL database: {e}')
                conn.rollback()
                raise

    def write_chunk(
            self,
            data,
            *args, **kwargs
    ) -> Iterator[Any]:
        self.write_all(data, *args, **kwargs)
        yield None

    def execute(self, statement_list: List[str], **line):
        with self.engine.connect() as conn:
            for statement in statement_list:
                conn.execute(text(statement), **line)

    def drop_table(
            self,
            table_name: str,
            ignore_fail: bool = True
    ):
        self.logging_manager.log_info(f"Dropping table '{table_name}'...")
        
        try:
            tbl = Table(
                table_name, MetaData(),
                autoload_with=self.engine
            )
        except sqlalchemy.exc.NoSuchTableError:
            if ignore_fail:
                self.logging_manager.log_warning(f"Table '{table_name}' not found, skipping drop operation.")
                return
            else:
                self.logging_manager.log_error(f"Table '{table_name}' not found.")
                raise
        
        try:
            tbl.drop(self.engine, checkfirst=False)
            self.logging_manager.log_info(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            self.logging_manager.log_error(f"Error dropping table '{table_name}': {e}")
            raise

    def create_table(
            self,
            table_name: str,
            columns,
            **params):
        self.logging_manager.log_info(f"Creating table '{table_name}'...")
        
        metadata = MetaData()
        self.logging_manager.log_debug(f'Columns: {columns}')
        print(columns)
        
        table = Table(table_name, metadata, *columns)
        
        try:
            metadata.create_all(self.engine, **params)
            self.logging_manager.log_info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            self.logging_manager.log_exception(f"Error creating table '{table_name}': {e}")
            raise
