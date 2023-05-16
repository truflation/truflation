"""
Reader
"""

from tfi.data.connector import Connector, ConnectorSql, \
    ConnectorCsv, ConnectorRest

Reader = Connector
ReaderSql = ConnectorSql
ReaderCsv = ConnectorCsv
ReaderRest = ConnectorRest
