from tfi.data.export_details import ExportDetails
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

'''
  Dev Notes
    In general, all databases should have the following fields
    identifiers -- identifiers used in getting identifiers-value pairs
        date -- a date object speficifying the date
        misc identifiers -- like country, position, name, color, source, et cetera
    value -- a value with consistent type, such as double or string
    createdAt -- a datetime object indicated when this data was added to the database
'''


class Exporter:
    """
    Exporter is a class that is able to export data to databases.
    """
    def __init__(self):
        pass

    def export(self, export_details: ExportDetails, df_local):
        """
        Export dataframe to database.

        param:
          export_details: ExportDetails: database details
          df_local: Pandas.DataFrame: dataframe to export
        """
        # Works but throws 'mariadb.ProgrammingError: Cursor is closed' error
        # sql_alchemy_uri = f"mariadb+mariadbconnector://{export_details.username}:{export_details.password}@127.0.0.1:{export_details.port}/{export_details.db}"

        # create created at for df if none exists (new data)
        if 'createdAt' not in df_local:
            df_local['createdAt'] = pd.to_datetime(pd.Timestamp.now(), unit='s')

        # Read in remote database as dataframe
        df_remote = self.read(export_details)

        # Reduce future created at to current time
        df_local = self.reduce_future_created_at(df_local)
        df_remote = self.reduce_future_created_at(df_remote)

        # If remote exists, reconcile and receive the data needing to be added
        df_new_data = self.reconcile_dataframes(df_remote, df_local) if df_remote is not None else df_local

        # print(f'df_remote\n{df_remote}')
        # print(f'df_new_data\n{df_new_data}')

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
        """
        Export dataframe to database. Theis replace the table in database with the dataframe.

        param:
          export_details: ExportDetails: database details
          df_local: Pandas.DataFrame: dataframe to export
        """

        sql_alchemy_uri = f'mariadb+pymysql://{export_details.username}:{export_details.password}@{export_details.host}:{export_details.port}/{export_details.db}'
        engine = create_engine(sql_alchemy_uri)
        df.to_sql(export_details.table, con=engine, if_exists='replace', chunksize=1000)

    @staticmethod
    def read(export_details: ExportDetails):
        """
        Read data from database

        param:
          export_details: ExportDetails: database details
        """
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
        """
        Reduces createdAt to present time for future values.

        param:
          df: Pandas.DataFrame: dataframe to reduce
        """

        if df is None or 'createdAt' not in df:
            return df
        # create mask for timestamps greater than now
        date_time_now = datetime.datetime.now()
        mask = df['createdAt'] > date_time_now
        # Update those rows
        df.loc[mask, 'createdAt'] = date_time_now
        return df

    @staticmethod
    def reconcile_dataframes(df_base, df_incoming):
        """
        Retrieve a dataframe that contains the rows needed to update df_base with the values from df_incoming

        param:
          df_base: Pandas.DataFrame: dataframe to add to
          df_incoming: Pandas.DataFrame: dataframe to add
        """
        # step 2 -- filter db to most recent (createdAt) date-value pairs
        df_base['date'] = pd.to_datetime(df_base['date'])  # make sure the 'date' column is in datetime format
        df_base = df_base.sort_values(['date', 'value', 'createdAt'], ascending=False).drop_duplicates(
            ['date', 'value']).sort_index()

        # Merge
        # ensure date columns are in datetime format
        df_base['date'] = pd.to_datetime(df_base['date'])
        df_incoming['date'] = pd.to_datetime(df_incoming['date'])

        identifiers = [x for x in df_base.columns if x not in ['createdAt']]

        # perform merge operation on 'date' and 'value'
        # df_merge = pd.merge(df_base, df_incoming, how='outer', on=['date', 'value'], indicator=True, suffixes=('', '_y'))
        df_merge = pd.merge(df_base, df_incoming, how='outer', on=identifiers, indicator=True, suffixes=('', '_y'))

        # filter rows that are in df_incoming only
        df_new_data = df_merge.loc[df_merge['_merge'] == 'right_only'].drop(columns=['_merge'])

        # remove the unnecessary index and createdAt_x column if exists
        if 'index' in df_new_data.columns:
            df_new_data = df_new_data.drop(columns=['index'])
        if 'createdAt' in df_new_data.columns:
            df_new_data = df_new_data.drop(columns=['createdAt'])

        # rename createdAt_y to createdAt
        df_new_data = df_new_data.rename(columns={"createdAt_y": "createdAt"})

        return df_new_data

    # todo -- consider making this take in only a dataframe
    # todo -- review, as this was ChatGPT originated
    def get_frozen_data(self, export_details: ExportDetails, frozen_datetime=None):
        """
        Get a dataframe from a database with the most recent date-value pairs such that:
            1. all dates at or before frozen_datetime must contain createdAt values before or equal to frozen_datetime
            2. all dates after frozen_datetime must contain createdAt values before or equal to the date in question
        Date-value pairs are immutable.

        param:
          export_details: ExportDetails: database details
          frozen_datetime: datetime.datetime: time in which we view snapshot.
        """

        # define frozen_date and frozen_datetime
        frozen_datetime = datetime.datetime.now() if frozen_datetime is None else frozen_datetime
        frozen_date = frozen_datetime.date()

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
        # df_b = df[cond_after_frozen_date] # all data that came after frozen_date, day by day
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
