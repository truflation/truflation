"""
Connector factory
"""

import os
from typing import Optional

from truflation.data.connectors import Connector, ConnectorGoogleSheets, ConnectorPandasDataReader, ConnectorSql,ConnectorCsv,ConnectorJson,ConnectorExcel,ConnectorDirect, Cache, ConnectorRest

cache_ = Cache()
connector_factory_list = []

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

def add_connector_factory(factory_function) -> None:
    connector_factory_list.append(factory_function)

def get_database_handle(db_type='mariadb+pymysql'):
    DB_USER = os.environ.get('DB_USER', None)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
    DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.environ.get('DB_PORT', None))
    DB_NAME = os.environ.get('DB_NAME', None)

    return f'{db_type}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
