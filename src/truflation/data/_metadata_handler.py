import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy import create_engine, select, desc, MetaData, Table, Column, VARCHAR, DATETIME
from truflation.data.logging_manager import Logger

class _MetadataHandler:
    def __init__(self, env_path = '../../../.env', engine=None):
        # Load environment variables from a .env file into the environment
        self.env_path = env_path
        self.logging_manager = Logger()

        if engine is None:
            # load_dotenv(self.env_path)
            load_dotenv(self.env_path)
            # Connect to database using environment variables
            self.db_url = f"mariadb+pymysql://{os.environ.get('DB_USER', None)}:{os.environ.get('DB_PASSWORD', None)}@{os.environ.get('DB_HOST', 'localhost')}:{os.environ.get('DB_PORT', None)}/{os.environ.get('DB_NAME', None)}"
            self.engine = create_engine(self.db_url)
        else:
            self.engine = engine

        # Create metadata
        self.metadata = MetaData()
        self.table = '_metadata'

        # Define _metadata keys
        self.key = ['category', 'name', 'latest_date', 'last_update']
        self.temporary_key = ['frequency', 'other']

        # List of table prefixes that should be excluded from _metadata processing
        self.blackList = ['__metadata__', '_metadata', 'categories', 'normalized']

        self.load_frequency()
        if len(self.frequency_data) > 0:
            self.create_table()

    def load_frequency(self):
        file_path = './frequency/frequency.json'
        if not os.path.exists(file_path):
            self.frequency_data = []
            return
        with open(file_path, 'r', encoding='utf-8') as frequency_json:
            self.frequency_data = json.load(frequency_json)

    def get_frequency_data(self, index_name = None):
        for item in self.frequency_data:
            if item['exact'] == 1 and index_name == item['index']:
                return item
            if item['exact'] == 0 and index_name.startswith(item['index']):
                return item
        return None

    def create_table(self):
        '''
        Create _metadata table if it does not exist
        '''
        # Define _metadata table
        self._metadata = Table(
            self.table,
            self.metadata,
            Column('table_name', VARCHAR(255)),
            Column('_key', VARCHAR(255)),
            Column('value', VARCHAR(255)),
            Column('value_type', VARCHAR(255)),
            Column('created_at', DATETIME),
            Column('updated_at', DATETIME),
        )

        try:
            # Create the table
            self.metadata.create_all(self.engine)
            self.logging_manager.log_info(
                f'Table {self.table} created successfully.'
            )

        except OperationalError as err:
            self.logging_manager.log_exception(
                f'An error occurred while creating {self.table} table: {err}'
            )

    def empty_metadata_table(self):
        with self.engine.connect() as conn:
            try:
                # Get the table object
                metadata_table = self.metadata.tables[self.table]
                conn.execute(metadata_table.delete())
                conn.commit()
                self.logging_manager.log_debug(f'Table {self.table} was emptied successfully.')
            except Exception as err:
                self.logging_manager.log_exception(
                    f'An error occurred while emptying {self.table} table: {err}'
                )
                conn.rollback()

    def reset(self):
        '''
        Reset the content of the _metadata table
        
        If it does not exist, create a new table, otherwise, empty the table content
        Add necessary metadata of all the tables existing in the database
        '''

        # Empty the _metadata table
        self.empty_metadata_table()

        # Reflect all tables
        self.metadata.reflect(bind=self.engine)

        try:
            # Fetch all tables from the database
            tables = self.metadata.tables.keys()
            self.logging_manager.log_info('Successfully fetched all tables from database.')

            # Iterate through each table in the database
            for table_name in tables:
                # Validate if the table is eligible for _metadata processing
                if self.validate_table(table_name):
                    self.add_index(table_name)

        except Exception as err:
            self.logging_manager.log_exception(
                f'An error occurred while fetching tables: {err}'
            )

    def validate_table(self, table_name):
        '''
        Check table name if it is valid for _metadata processing
        '''

        # Check if the table name starts with any blacklisted prefix
        for item in self.blackList:
            if table_name.startswith(item):
                return False

        return True

    def add_index(self, index_name):
        '''
        Add new metadata for new index
        '''

        for key_item in self.key:
            self.update_index(index_name, key_item)

        frequency = self.get_frequency_data(index_name)

        if frequency is not None:
            for key_item in self.temporary_key:
                self.update_index(index_name, key_item, frequency[key_item])


    def update_index(self, table_name, key, value = None):
        '''
        Update metadata for specific index
        '''

        value_type = 'string'

        # Populate metadata values based on the key
        if value is None:
            if key == 'category':
                value = '_'.join(table_name.split('_')[:2])
            elif key == 'name':
                value = '_'.join(table_name.split('_')[2:])
            elif key in {'latest_date', 'last_update'}:
                with self.engine.connect() as conn:
                    try:
                        # Retrieve the latest data from the table
                        # table_item = self.metadata.tables.get(table_name)
                        table_item = Table(table_name, self.metadata, autoload_with = self.engine)
                        query = select(table_item.c.date, table_item.c.created_at).order_by(desc(table_item.c.date))
                        result = conn.execute(query).fetchone()

                        if key == 'latest_date':
                            value = result[0].strftime('%Y-%m-%d') if result[0] else None
                            value_type = 'date'
                        elif key == 'last_update':
                            value = result[1].strftime('%Y-%m-%d %H:%M:%S')
                            value_type = 'datetime'
                    except (NoSuchTableError, OperationalError) as err:
                        self.logging_manager.log_exception(
                            f'An error occurred while getting data from {table_name} table: {err}'
                        )
                        conn.rollback()

        if value is not None:
            self.insert_row(table_name, key, value, value_type)

    def insert_row(self, table_name, key, value, value_type):
        # Check if the row exists in the _metadata table and perform insertion or update
        # _metadata_table = self.metadata.tables[self.table]
        _metadata_table = Table(self.table, self.metadata, autoload_with = self.engine)
        query = select(_metadata_table).where(_metadata_table.c.table_name == table_name).where(_metadata_table.c._key == key)
        with self.engine.connect() as conn:
            try:
                # with self.engine.connect() as connection:
                result = conn.execute(query).fetchone()

                if result:
                    # Row exists, perform update
                    update_query = _metadata_table.update().where(_metadata_table.c.table_name == table_name).where(_metadata_table.c._key == key).values(
                        value = value,
                        value_type = value_type,
                        updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    )
                    conn.execute(update_query)
                    conn.commit()
                    self.logging_manager.log_debug(f'Row updated {table_name} {key} successfully into {_metadata_table} table')
                else:
                    # Row doesn't exist, perform insert
                    insert_query = _metadata_table.insert().values(
                        table_name = table_name,
                        _key = key,
                        value = value,
                        value_type = value_type,
                        created_at = datetime.now(timezone.utc).replace(tzinfo=None),
                        updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    )
                    conn.execute(insert_query)
                    conn.commit()
                    self.logging_manager.log_debug(f'Row inserted {table_name} {key} successfully into {_metadata_table} table')
            except OperationalError as err:
                self.logging_manager.log_exception(
                    f'An error occurred while checking row existence or updating/inserting row: {err}'
                )
                # rollbacks are necessary to prevent timeouts
                conn.rollback()
