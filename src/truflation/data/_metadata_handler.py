import os
import json
import datetime
from icecream import ic
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, select, desc, MetaData, Table, Column, VARCHAR, DATETIME

class _MetadataHandler:
    def __init__(self, env_path = '../../../.env'):
        # Load environment variables from a .env file into the environment
        self.env_path = env_path
        
        load_dotenv(self.env_path)
        
        # Connect to database using environment variables
        self.db_url = f"mariadb+pymysql://{os.environ.get('DB_USER', None)}:{os.environ.get('DB_PASSWORD', None)}@{os.environ.get('DB_HOST', 'localhost')}:{os.environ.get('DB_PORT', None)}/{os.environ.get('DB_NAME', None)}"
        
        # Create the engine
        self.engine = create_engine(self.db_url)
        
        # Create metadata
        self.metadata = MetaData()
        
        # Create a session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
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
        with open(file_path, 'r') as frequency_json:
            self.frequency_data = json.load(frequency_json)
    
    def get_frequency_data(self, index_name = None):
        for item in self.frequency_data:
            if index_name.startswith(item['index']):
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
            ic(f'Table {self.table} created successfully.')
            
        except OperationalError as err:
            ic(f'An error occurred while creating {self.table} table: {err}')
    
    def empty_metadata_table(self):
        try:
            # Get the table object
            metadata_table = self.metadata.tables[self.table]
            
            # Create a connection
            self.session.execute(metadata_table.delete())
            ic(f'Table {self.table} was emptied successfully.')
            
        except Exception as err:
            ic(f'An error occurred while emptying {self.table} table: {err}')

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
            ic('Successfully fetched all tables from database.')
            
            # Iterate through each table in the database
            for table_name in tables:
                # Validate if the table is eligible for _metadata processing
                if self.validate_table(table_name):
                    self.add_index(table_name)
            
        except Exception as err:
            ic(f'An error occurred while fetching tables: {err}')
        
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
        if len(self.frequency_data) == 0:
            return

        # Create the _metadata table if not exists
        #for key_item in self.key:
        #    self.update_index(index_name, key_item)
        
        frequency = self.get_frequency_data(index_name)
        ic(frequency)
        if frequency is not None:
            for key_item in self.temporary_key:
                self.update_index(index_name, key_item, frequency[key_item])
        
        self.session.commit()

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
            elif key == 'latest_date' or key == 'last_update':
                try:
                    # Retrieve the latest data from the table
                    table_item = self.metadata.tables.get(table_name)
                    if table_item:
                        query = select(table_item.c.date, table_item.c.created_at).order_by(desc(table_item.c.date))
                        result = self.session.execute(query).fetchone()
                        if key == 'latest_date':
                            value = result[0].strftime('%Y-%m-%d') if result[0] else None
                            value_type = 'date'
                        
                        elif key == 'last_update':
                            value = result[1].strftime('%Y-%m-%d %H:%M:%S')
                            value_type = 'datetime'
                except Exception as err:
                    ic(f'An error occurred while getting data from {table_name} table: {err}')

        if value is not None:
            self.insert_row(table_name, key, value, value_type)

    def insert_row(self, table_name, key, value, value_type):
        # Check if the row exists in the _metadata table and perform insertion or update
        _metadata_table = self.metadata.tables[self.table]
        query = select(_metadata_table).where(_metadata_table.c.table_name == table_name).where(_metadata_table.c._key == key)
        
        try:
            # with self.engine.connect() as connection:
            result = self.session.execute(query).fetchone()
            
            if result:
                # Row exists, perform update
                update_query = _metadata_table.update().where(_metadata_table.c.table_name == table_name).where(_metadata_table.c._key == key).values(
                    value = value,
                    value_type = value_type,
                    updated_at = datetime.datetime.utcnow()
                )
                self.session.execute(update_query)
                ic(f'Row updated successfully {value} into {_metadata_table} table')
            
            else:
                # Row doesn't exist, perform insert
                insert_query = _metadata_table.insert().values(
                    table_name = table_name,
                    _key = key,
                    value = value,
                    value_type = value_type,
                    created_at = datetime.datetime.utcnow(),
                    updated_at = datetime.datetime.utcnow()
                )
                self.session.execute(insert_query)
                ic(f'Row inserted successfully {value} into {_metadata_table} table')

        except OperationalError as err:
            ic(f'An error occurred while checking row existence or updating/inserting row: {err}')
