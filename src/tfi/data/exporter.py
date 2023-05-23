from tfi.data.connector import connector_factory
from tfi.data.export_details import ExportDetails
# import the module
from sqlalchemy import create_engine, types, MetaData, Table, Column
from sqlalchemy.dialects.mysql import DATETIME, DATE
import pymysql
import pandas as pd
# pip install SQLAlchemy
# pip install PyMySQL
# pip install mariadb
# sudo apt install libmariadb3 libmariadb-dev # needed for mariadb installation
import time
import datetime

class Exporter:
    def __init__(self):
        pass

    def export(self, export_details: ExportDetails, df_local):

        # Works but throws 'mariadb.ProgrammingError: Cursor is closed' error
        # sql_alchemy_uri = f"mariadb+mariadbconnector://{export_details.username}:{export_details.password}@127.0.0.1:{export_details.port}/{export_details.db}"

        # create created at for df if none exists (new data)
        if 'createdAt' not in df_local:
            df_local['createdAt'] = pd.to_datetime(pd.Timestamp.now(), unit='s')

        print(f'df_local\n{df_local}')

        # Read in remote database as dataframe
        df_remote = self.read(export_details)


        # Reduce future created at to curren time
        df_local = self.reduce_future_created_at(df_local)
        df_remote = self.reduce_future_created_at(df_remote)


        # If remote exists, reconcile and receive the data needing to be added
        df_new_data = self.reconcile_dataframes(df_remote, df_local) if df_remote is not None else df_local

        print(f'df_remote\n{df_remote}')
        print(f'df_new_data\n{df_new_data}')


        if 'date' in df_local:
            df_local['date'] = pd.to_datetime(df_local['date'])  # make sure the 'date' column is in datetime format


        # # Initialize metadata object
        # metadata = MetaData()
        #
        # # Define the table
        # table = Table(
        #     export_details.table, metadata,
        #     Column('date', DATE()),  # specifying fractional seconds precision
        #     Column('createdAt', DATETIME(fsp=6)),  # specifying fractional seconds precision
        #     # add other columns as needed
        # )


        # Insert
        sql_alchemy_uri = f'mariadb+pymysql://{export_details.username}:{export_details.password}@{export_details.host}:{export_details.port}/{export_details.db}'
        engine = create_engine(sql_alchemy_uri)
        # metadata.create_all(engine)
        df_new_data.to_sql(export_details.table,
                           con=engine,
                           if_exists='append',
                           chunksize=1000,
                           index=False,  # change to True if you want to write the index into a separate column
                           dtype={
                               # 'createdAt': types.DateTime(precision=6),
                               'date': types.Date(),
                               # 'createdAt': types.TIMESTAMP() # TIMESTAMP is limited to 2038
                               'createdAt': types.DATETIME()  # TIMESTAMP is limited to 2038
                           }
                           )

    @staticmethod
    def export_dump(export_details: ExportDetails, df):
        sql_alchemy_uri = f'mariadb+pymysql://{export_details.username}:{export_details.password}@{export_details.host}:{export_details.port}/{export_details.db}'
        engine = create_engine(sql_alchemy_uri)
        df.to_sql(export_details.table, con=engine, if_exists='replace', chunksize=1000)

    @staticmethod
    def read(export_details: ExportDetails):
        # Connect to the database
        connection = pymysql.connect(host=export_details.host,
                                     user=export_details.username,
                                     password=export_details.password,
                                     db=export_details.db)

        # create cursor
        my_cursor = connection.cursor()

        # Check if table exists
        my_cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(export_details.table.replace('\'', '\'\'')))
        if my_cursor.fetchone()[0] == 0:
            my_cursor.close()
            return None


        # Get row names
        my_cursor.execute(f"SHOW columns FROM {export_details.table}")
        column_names_info = my_cursor.fetchall()
        column_names = [x[0] for x in column_names_info]

        # Execute Query
        my_cursor.execute(f"SELECT * from {export_details.table}")

        # Fetch the records
        data = my_cursor.fetchall()

        # Convert to dataframe
        df = pd.DataFrame(data, columns=column_names)

        # Close the connection
        connection.close()

        return df

    @staticmethod
    def reduce_future_created_at(df):

        if df is None or 'createdAt' not in df:
            return df
        # create mask for timestamps greater than now
        date_time_now = datetime.datetime.now()
        mask = df['createdAt'] > date_time_now
        # Update those rows
        df.loc[mask, 'createdAt'] = date_time_now
        return df

    # @staticmethod
    # def reconcile_dataframes(df_base, df_other):
    #     # filter db to most recent (createdAt) date-value pairs
    #     df_base['date'] = pd.to_datetime(df_base['date'])  # make sure the 'date' column is in datetime format
    #     df_base = df_base.sort_values('createdAt', ascending=False).drop_duplicates('date').sort_index()
    #
    #     # ensure date columns are in datetime format
    #     df_base['date'] = pd.to_datetime(df_base['date'])
    #     df_other['date'] = pd.to_datetime(df_other['date'])
    #
    #     # perform merge operation on 'date', 'value' and 'createdAt'
    #     df_merge = pd.merge(df_base, df_other, how='outer', on=['date', 'value'], indicator=True)
    #
    #     # filter rows that are in df_other only
    #     df_new_data = df_merge.loc[df_merge['_merge'] == 'right_only'].drop(columns=['_merge'])
    #
    #     # remove the unnecessary index column if exists
    #     if 'index' in df_new_data.columns:
    #         df_new_data = df_new_data.drop(columns=['index'])
    #
    #     print(f'df_merge: \n{df_merge}')
    #
    #     return df_new_data



    # @staticmethod
    # def reconcile_dataframes(df_base, df_other):
    #     # step 2 -- filter db to most recent (createdAt) date-value pairs
    #     df_base['date'] = pd.to_datetime(df_base['date'])  # make sure the 'date' column is in datetime format
    #     df_base = df_base.sort_values(['date', 'value', 'createdAt'], ascending=False).drop_duplicates(
    #         ['date', 'value']).sort_index()
    #
    #     # Merge
    #     # ensure date columns are in datetime format
    #     df_base['date'] = pd.to_datetime(df_base['date'])
    #     df_other['date'] = pd.to_datetime(df_other['date'])
    #
    #     # perform merge operation on 'date' and 'value'
    #     df_merge = pd.merge(df_base, df_other, how='outer', on=['date', 'value'], indicator=True)
    #
    #     # filter rows that are in df_other only
    #     df_new_data = df_merge.loc[df_merge['_merge'] == 'right_only'].drop(columns=['_merge'])
    #
    #     # remove the unnecessary index column if exists
    #     if 'index' in df_new_data.columns:
    #         df_new_data = df_new_data.drop(columns=['index'])
    #
    #     # append df_new_data to df_base
    #     # df_base_updated = df_base.append(df_new_data, ignore_index=True)
    #     return df_new_data

    @staticmethod
    def reconcile_dataframes(df_base, df_other):
        # step 2 -- filter db to most recent (createdAt) date-value pairs
        df_base['date'] = pd.to_datetime(df_base['date'])  # make sure the 'date' column is in datetime format
        df_base = df_base.sort_values(['date', 'value', 'createdAt'], ascending=False).drop_duplicates(
            ['date', 'value']).sort_index()

        # Merge
        # ensure date columns are in datetime format
        df_base['date'] = pd.to_datetime(df_base['date'])
        df_other['date'] = pd.to_datetime(df_other['date'])

        # perform merge operation on 'date' and 'value'
        df_merge = pd.merge(df_base, df_other, how='outer', on=['date', 'value'], indicator=True, suffixes=('', '_y'))

        # filter rows that are in df_other only
        df_new_data = df_merge.loc[df_merge['_merge'] == 'right_only'].drop(columns=['_merge'])

        # remove the unnecessary index and createdAt_x column if exists
        if 'index' in df_new_data.columns:
            df_new_data = df_new_data.drop(columns=['index'])
        if 'createdAt' in df_new_data.columns:
            df_new_data = df_new_data.drop(columns=['createdAt'])

        # rename createdAt_y to createdAt
        df_new_data = df_new_data.rename(columns={"createdAt_y": "createdAt"})

        return df_new_data

    # todo -- review, as this was ChatGPT originated
    def get_frozen_data(self, export_details: ExportDetails, frozen_datetime=None):
        # define frozen_date and frozen_datetime
        # frozen_datetime = datetime.datetime.now().timestamp() if frozen_datetime is None else frozen_datetime
        frozen_datetime = datetime.datetime.now() if frozen_datetime is None else frozen_datetime
        # frozen_date = datetime.datetime.fromtimestamp(frozen_datetime)  # .date()
        frozen_date = frozen_datetime.date()  # .date()
        # frozen_datetime = pd.to_datetime(frozen_datetime, unit='s')

        df = self.read(export_details)
        df['date'] = pd.to_datetime(df['date'])  # make sure the 'date' column is in datetime format

        # Create new column for the end of the day
        df['endOfDayDatetime'] = (df['date'] + pd.DateOffset(days=1) - pd.Timedelta(seconds=1))
        # df['endOfDayDatetime'] = (df['date'] + pd.DateOffset(days=1) - pd.Timedelta(seconds=1)).apply(lambda x: x.timestamp())

        # create conditions for the filter
        cond_before_frozen_date = (df['date'].dt.date <= frozen_date) & (df['createdAt'] <= frozen_datetime)
        cond_after_frozen_date = (df['date'].dt.date > frozen_date) & (df['createdAt'] <= df['endOfDayDatetime'])

        # apply the filter
        # df_a = df[cond_before_frozen_date] # Original
        # df_b = df[cond_after_frozen_date] # all data that came after, day by day
        df = df[cond_before_frozen_date | cond_after_frozen_date]

        # reduce the DataFrame to only contain rows with the latest 'createdAt'
        df = df.sort_values('createdAt', ascending=False).drop_duplicates('date').sort_index()

        del df['endOfDayDatetime']

        if 'index' in df.columns:
            df = df.drop(columns=['index'])

        return df


# ChatGPT Snippet -- gets the most recent data
# todo -- have a created at function that removes all
'''# assuming df is your DataFrame and it has columns 'date', 'value', and 'createdAt'
df['date'] = pd.to_datetime(df['date'])  # make sure the 'date' column is in datetime format
df = df.sort_values('createdAt', ascending=False).drop_duplicates('date').sort_index()
'''


# ChatGPT Snippet --- FrozenDate
'''
import pandas as pd

# assuming df is your DataFrame and it has columns 'date', 'value', 'createdAt', 'endOfDayTimestamp'

df['date'] = pd.to_datetime(df['date'])  # make sure the 'date' column is in datetime format

# define your frozen_date and frozen_timestamp
frozen_date = pd.to_datetime('yyyy-mm-dd')  # replace with actual frozen date
frozen_timestamp = 123456789.123456  # replace with actual frozen timestamp

# create conditions for the filter
cond_before_frozen_date = (df['date'] < frozen_date) & (df['createdAt'] < frozen_timestamp)
cond_after_frozen_date = (df['date'] > frozen_date) & (df['createdAt'] < df['endOfDayTimestamp'])

# apply the filter
df = df[cond_before_frozen_date | cond_after_frozen_date]

# reduce the DataFrame to only contain rows with the latest 'createdAt'
df = df.sort_values('createdAt', ascending=False).drop_duplicates('date').sort_index()
'''
