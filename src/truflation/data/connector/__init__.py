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

# todo -- I think we should have a dedicated Connector for Excel, just as we do for csv
# I like have abstraction for http links, but I find it troublesome for its potential misinterpretation
def connector_factory(connector_type: str) -> Optional[Connector]:
    if connector_type.startswith('cache'):
        return cache_.connector()
    if connector_type.startswith('object'):
        return ConnectorDirect()
    if connector_type == 'excel':
        return ConnectorExcel()
    if connector_type == 'csv':
        return ConnectorCsv()
    if connector_type.startswith('csv:'):
        path_root = connector_type.split(':', 1)[1]
        return ConnectorCsv(path_root=path_root)
    if connector_type.startswith('gsheet'):
        if connector_type.startswith('gsheet:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorGoogleSheets(path_root=path_root)
        return ConnectorGoogleSheets()
    if connector_type.startswith('json'):
        if connector_type.startswith('json:'):
            path_root = connector_type.split(':', 1)[1]
            return ConnectorJson(path_root=path_root)
        return ConnectorJson()
    if connector_type.startswith('playwright+http'):
        # return ConnectorRest(source_location, playwright=True)
        return ConnectorRest(playwright=True)
    if connector_type.startswith('rest+http'):
        return ConnectorRest()
    if connector_type.startswith('http'):
        return ConnectorRest(json=False)
    if connector_type.startswith('csv+http'):
        return ConnectorRest(csv=True)
    if connector_type.startswith('sqlite') or \
            connector_type.startswith('postgresql') or \
            connector_type.startswith('mysql') or \
            connector_type.startswith('mariadb') or \
            connector_type.startswith('oracle') or \
            connector_type.startswith('mssql') or \
            connector_type.startswith('sqlalchemy') or \
            connector_type.startswith('gsheets') or \
            connector_type.startswith('pybigquery'):
        return ConnectorSql(connector_type)
    if connector_type.startswith('pandas_datareader'):
        return ConnectorPandasDataReader()
    for factory in connector_factory_list:
        result = factory(connector_type)
        if result is not None:
            return result
    return None
