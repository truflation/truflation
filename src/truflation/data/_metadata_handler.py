import os
import json
import datetime
import mysql.connector
from dotenv import load_dotenv

class _MetadataHandler:
    def __init__(self, env_path = '../../../.env'):
        # Load environment variables from a .env file into the environment
        self.env_path = env_path
        
        load_dotenv(self.env_path)
        
        # Connect to database using environment variables
        self.db_connection = mysql.connector.connect(
            host = os.getenv('DB_HOST') | 'localhost',
            user = os.getenv('DB_USER') | 'root',
            password = os.getenv('DB_PASSWORD') | 'password12321',
            database = os.getenv('DB_NAME') | 'timeseries'
        )
        
        if self.db_connection.is_connected():
            print("Successfully connected to main database.")
            self.cursor = self.db_connection.cursor()
        
        self.table = '_metadata'
        
        # Define _metadata keys
        self.key = ['category', 'name', 'latest_date', 'last_update']
        self.temporary_key = ['frequency', 'other']
        
        # List of table prefixes that should be excluded from _metadata processing
        self.blackList = ['__metadata__', '_metadata', 'categories', 'normalized']
        
        self.load_frequency()

    def load_frequency(self):
        with open('./frequency/frequency.json', 'r') as frequency_json:
            self.frequency_data = json.load(frequency_json)
    
    def get_frequency_data(self, index_name = None):
        for i in range(0, len(self.frequency_data)):
            if index_name.startswith(self.frequency_data[i]['index']):
                return self.frequency_data[i]
        return None

    def create_table(self):
        '''
        Create _metadata table if it does not exist
        '''

        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {self.table} (
                table_name VARCHAR(255),
                _key VARCHAR(255),
                value VARCHAR(255),
                value_type VARCHAR(255),
                created_at DATETIME,
                updated_at DATETIME
            )
        '''

        try:
            # Execute the SQL query to create the table
            self.cursor.execute(create_table_query)
            print(f'Successfully created {self.table} table')
            
        except mysql.connector.Error as err:
            print(f'An error occurred while creating {self.table} table: {err}')


    def reset(self):
        '''
        Reset the content of the _metadata table
        
        If it does not exist, create a new table, otherwise, empty the table content
        Add necessary metadata of all the tables existing in the database
        '''
        
        # Create the _metadata table
        self.create_table()
        
        # Empty the _metadata table
        query = f'delete from {self.table}'
        self.cursor.execute(query)
        
        # Fetch all tables from the database
        query = 'show tables'
        
        try:
            self.cursor.execute(query)
            tables = self.cursor.fetchall()
            
            print('Successfully fetch all tables from database')

            # Iterate through each table in the database
            for table_item in tables:
                table_name = table_item[0]
                
                # Validate if the table is eligible for _metadata processing
                if self.validate_table(table_name):
                    self.add_index(table_name)
        except mysql.connector.Error as err:
            print(f'An error occurred while resetting {self.table} table: {err}')


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
        
        # Create the _metadata table if not exists
        self.create_table()
        
        for key_item in self.key:
            self.update_index(index_name, key_item)
        
        frequency = self.get_frequency_data(index_name)
        
        if frequency is not None:
            for key_item in self.temporary_key:
                self.update_index(index_name, key_item, frequency[key_item])

    def update_index(self, table_name, key, value = ''):
        '''
        Update metadata for specific index
        '''
        
        value_type = 'string'
        
        # Populate metadata values based on the key
        if not value:
            if key == 'category':
                value = '_'.join(table_name.split('_')[:2])
            elif key == 'name':
                value = '_'.join(table_name.split('_')[2:])
            elif key == 'latest_date' or key == 'last_update':
                try:
                    # Retrieve the latest data from the table
                    if key == 'latest_date':
                        self.cursor.execute(f'select date from {table_name} order by date desc limit 1;')
                        result = self.cursor.fetchone()
                        
                        value = result[0].strftime('%Y-%m-%d')
                        value_type = 'date'
                        
                    elif key == 'last_update':
                        self.cursor.execute(f'select created_at from {table_name} order by date desc limit 1;')
                        result = self.cursor.fetchone()
                        
                        value = result[0].strftime('%Y-%m-%d %H:%M:%S')
                        value_type = 'datetime'
                except mysql.connector.Error as err:
                    print(f'An error occurred while getting data from {table_name} table: {err}')
        
        self.insert_row(table_name, key, value, value_type)

    def insert_row(self, table_name, key, value, value_type):
        # Check if the row exists in the _metadata table and perform insertion or update
        query = f'''
            SELECT COUNT(*) FROM {self.table}
            WHERE table_name = '{table_name}' AND _key = '{key}'
        '''
        
        try:
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            
            if count > 0:
                update_query = f'''
                    UPDATE {self.table}
                    SET value = '{value}', value_type = '{value_type}', updated_at = '{datetime.datetime.utcnow()}'
                    WHERE table_name = '{table_name}' AND _key= '{key}'
                '''
                
                try:
                    self.cursor.execute(update_query)
                    print(f'Row updated Successfully at {table_name} table')
                except mysql.connector.Error as err:
                    print(f'An error occurred while updating row at {table_name} table: {err}')
            else:
                insert_query = f'''
                    INSERT INTO {self.table} (table_name, _key, value, value_type, created_at, updated_at)
                    VALUES ('{table_name}', '{key}', '{value}', '{value_type}', '{datetime.datetime.utcnow()}', '{datetime.datetime.utcnow()}')
                '''
                
                try:
                    self.cursor.execute(insert_query)
                    print(f'Row inserted successfully at {table_name} table')
                except mysql.connector.Error as err:
                    print(f'An error occurred while inserting row at {table_name} table: {err}')
        except mysql.connector.Error as err:
            print(f'An error occurred while inserting row to {self.table} table: {err}')
        
        # Commit the transaction
        self.db_connection.commit()