"""
Database handle util
"""

import os

def get_database_handle(db_type='mariadb+pymysql'):
    DB_USER = os.environ.get('DB_USER', None)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', None)
    DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.environ.get('DB_PORT', None))
    DB_NAME = os.environ.get('DB_NAME', None)

    return f'{db_type}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
