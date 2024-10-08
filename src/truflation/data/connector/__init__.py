import os
from typing import Optional
from .base import Connector
from .cache import Cache, ConnectorCache
from .csv import ConnectorCsv
from .direct import ConnectorDirect
from .excel import ConnectorExcel
from .gsheet import ConnectorGoogleSheets
from .json import ConnectorJson
from .kwil import ConnectorKwil
from .pandas_datareader import ConnectorPandasDataReader
from .rest_to_csv import RestToCsvConnector
from .rest import ConnectorRest
from .sql import ConnectorSql
from .factory import connector_factory_list, add_connector_factory
from .db_handle import get_database_handle

cache_ = Cache()

def connector_factory(connector_type: str) -> Optional[Connector]:
    # Dictionary mapping for simple cases
    connector_mapping = {
        'excel': ConnectorExcel,
        'rest+http': ConnectorRest,
        'pandas_datareader': ConnectorPandasDataReader,
        'cache': cache_.connector,
        'object': ConnectorDirect,
    }
    
    # Return connectors directly from the mapping
    if connector_type in connector_mapping:
        return connector_mapping[connector_type]()

    # Handle prefixes
    prefix_mapping = {
        'gsheet': lambda path: ConnectorGoogleSheets(path_root=path),
        'csv': lambda path: ConnectorCsv(path_root=path),
        'json': lambda path: ConnectorJson(path_root=path),
        'playwright+http': lambda _: ConnectorRest(playwright=True),
        'http': lambda _: ConnectorRest(json=False),
        'csv+http': lambda _: ConnectorRest(csv=True),
    }

    for prefix, factory in prefix_mapping.items():
        if connector_type.startswith(prefix):
            if ':' in connector_type:
                path_root = connector_type.split(':', 1)[1]
                return factory(path_root)
            return factory(os.getcwd())

    # SQL connectors handling
    sql_prefixes = ['sqlite', 'postgresql', 'mysql', 'mariadb', 'oracle', 'mssql', 'sqlalchemy', 'pybigquery']
    if any(connector_type.startswith(prefix) for prefix in sql_prefixes):
        return ConnectorSql(connector_type)

    # Try external connector factories
    for factory in connector_factory_list:
        result = factory(connector_type)
        if result:
            return result

    return None
