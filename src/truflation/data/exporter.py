import datetime
import logging
import pandas
from truflation.data.export_details import ExportDetails
from truflation.data.logging_manager import Logger
from sqlalchemy import types

'''
  Dev Notes
    In general, all databases should have the following fields
    identifiers -- identifiers used in getting identifiers-value pairs
        date -- a date object speficifying the date
        misc identifiers -- like country, position, name, color, source, et cetera
    value -- a value with consistent type, such as double or string
    created_at -- a datetime object indicated when this data was added to the database
'''


class Exporter:
    """
    Exporter is a class that is able to export data to databases.
    """
    def __init__(self):
        self.logging_manager = Logger()

    def export(self, export_details: ExportDetails, df_local: pandas.DataFrame, dry_run=False) -> pandas.DataFrame:
        """
        Export dataframe to database.

        param:
          export_details: ExportDetails: database details
          df_local: Pandas.DataFrame: dataframe to export
        """
        # Works but throws 'mariadb.ProgrammingError: Cursor is closed' error
        # sql_alchemy_uri = f"mariadb+mariadbconnector://{export_details.username}:{export_details.password}@127.0.0.1:{export_details.port}/{export_details.db}"

        if not isinstance(df_local, pandas.DataFrame):
            export_details.write(
                df_local
            )
            return

        # create created at for df if none exists (new data)
        if 'created_at' not in df_local:
            df_local['created_at'] = pandas.to_datetime(datetime.datetime.utcnow())
        else:
            df_local['created_at'] = pandas.to_datetime(df_local['created_at'])

        # Read in remote database as dataframe
        df_remote = export_details.read()

        # Reduce future created at to current time
        df_local = self.reduce_future_created_at(df_local)
        df_remote = self.reduce_future_created_at(df_remote)

        # If remote exists, reconcile and receive the data needing to be added
        reconcile = self.reconcile_dataframes if export_details.reconcile is None else export_details.reconcile
        df_new_data = reconcile(df_remote, df_local) if df_remote is not None and not df_remote.empty else df_local
        if not df_new_data.empty:
            self.logging_manager.log_info('exporting....')
            self.logging_manager.log_info(df_new_data)

        if 'date' in df_local:
            df_local['date'] = pandas.to_datetime(df_local['date'])  # make sure the 'date' column is in datetime format

        if not dry_run and not df_new_data.empty:
            # Insert
            if export_details.create_table is None:
                export_details.write(
                    df_new_data,
                    chunksize=1000,
                    index= (df_new_data.index.name == 'date'),
                    dtype={
                        # 'created_at': types.DateTime(precision=6),
                    'date': types.Date(),
                        'created_at': types.DATETIME()
                },
                )
            else:
                export_details.create_table(
                    export_details,
                    df_new_data
                )

        return df_new_data

    @staticmethod
    def export_dump(export_details: ExportDetails, df: pandas.DataFrame) -> None:
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
    def reduce_future_created_at(df: pandas.DataFrame) -> pandas.DataFrame :
        """
        Reduces created_at to present time for future values.

        param:
          df: Pandas.DataFrame: dataframe to reduce
        """

        if df is None or 'created_at' not in df:
            return df
        # create mask for timestamps greater than now
        date_time_now = datetime.datetime.utcnow()
        df['created_at'] = pandas.to_datetime(df['created_at'])
        mask = df['created_at'] > date_time_now
        # Update those rows
        df.loc[mask, 'created_at'] = date_time_now
        return df

    @staticmethod
    def reconcile_dataframes(df_base: pandas.DataFrame, df_incoming: pandas.DataFrame, rounding:int = 6) -> pandas.DataFrame :
        """
        Retrieve a dataframe that contains the rows needed to update df_base with the values from df_incoming

        param:
          df_base: Pandas.DataFrame: dataframe to add to
          df_incoming: Pandas.DataFrame: dataframe to add
        """

        # Merge
        # ensure date columns are in datetime format
        if df_incoming.index.name == 'date':
            df_incoming = df_incoming.reset_index() # reset index if date is being used as index
        df_base['date'] = pandas.to_datetime(df_base['date'])
        df_incoming['date'] = pandas.to_datetime(df_incoming['date'])

        identifiers = [x for x in df_base.columns if x not in ['created_at']]

        try:
            df_new_data = df_incoming.round(rounding).merge(
                df_base[identifiers].round(rounding),
                on=identifiers,
                how='left',
                indicator=True
            )
            df_new_data = df_new_data[
                df_new_data['_merge']=='left_only'
            ].drop('_merge', axis=1)
        except ValueError as e:
            self.logging_manager.log_exception(df_base.info())
            self.logging_manager.log_exception(df_incoming.info())
            raise e

        if 'index' in df_new_data.columns:
            df_new_data = df_new_data.drop(columns=['index'])
        # drop duplicates
        columns = list(df_new_data.columns.values)
        columns_filtered = [
            item for item in columns \
            if item != 'created_at'
        ]
        df_new_data = df_new_data.sort_values(columns, ascending=True).drop_duplicates(
            subset=columns_filtered
        )
        df_new_data = df_new_data[df_base.columns].set_index(['date'])
        return df_new_data


    # todo -- consider making this take in only a dataframe
    # todo -- review, as this was ChatGPT originated
    def get_frozen_data(self, export_details: ExportDetails, frozen_datetime: datetime.datetime = None) -> pandas.DataFrame:
        """
        Get a dataframe from a database with the most recent date-value pairs such that:
            1. all dates at or before frozen_datetime must contain created_at values before or equal to frozen_datetime
            2. all dates after frozen_datetime must contain created_at values before or equal to the date in question
        Date-value pairs are immutable.

        param:
          export_details: ExportDetails: database details
          frozen_datetime: datetime.datetime: time in which we view snapshot.
        """

        # define frozen_date and frozen_datetime
        frozen_datetime = datetime.datetime.utcnow() if frozen_datetime is None else frozen_datetime
        frozen_date = frozen_datetime.date()

        df = export_details.read()
        df['date'] = pandas.to_datetime(df['date'])  # make sure the 'date' column is in datetime format

        # Create new column for the end of the day
        df['endOfDayDatetime'] = (df['date'] + pandas.DateOffset(days=1) - pandas.Timedelta(seconds=1))
        # df['endOfDayDatetime'] = (df['date'] + pandas.DateOffset(days=1) - pandas.Timedelta(seconds=1)).apply(lambda x: x.timestamp())

        # create conditions for the filter
        cond_before_frozen_date = (df['date'].dt.date <= frozen_date) & (df['created_at'] <= frozen_datetime)
        cond_after_frozen_date = (df['date'].dt.date > frozen_date) & (df['created_at'] <= df['endOfDayDatetime'])

        # apply the filter
        # df_a = df[cond_before_frozen_date] # Original
        # df_b = df[cond_after_frozen_date] # all data that came after frozen_date, day by day
        df = df[cond_before_frozen_date | cond_after_frozen_date]

        # reduce the DataFrame to only contain rows with the latest 'created_at'
        df = df.sort_values('created_at', ascending=False).drop_duplicates('date').sort_index()

        del df['endOfDayDatetime']

        if 'index' in df.columns:
            df = df.drop(columns=['index'])

        return df


# ChatGPT Snippet -- gets the most recent data
# todo -- have a created at function that removes all
'''# assuming df is your DataFrame and it has columns 'date', 'value', and 'created_at'
df['date'] = pandas.to_datetime(df['date'])  # make sure the 'date' column is in datetime format
df = df.sort_values('created_at', ascending=False).drop_duplicates('date').sort_index()
'''


# ChatGPT Snippet --- FrozenDate
'''
import pandas as pd

# assuming df is your DataFrame and it has columns 'date', 'value', 'created_at', 'endOfDayTimestamp'

df['date'] = pandas.to_datetime(df['date'])  # make sure the 'date' column is in datetime format

# define your frozen_date and frozen_timestamp
frozen_date = pandas.to_datetime('yyyy-mm-dd')  # replace with actual frozen date
frozen_timestamp = 123456789.123456  # replace with actual frozen timestamp

# create conditions for the filter
cond_before_frozen_date = (df['date'] < frozen_date) & (df['created_at'] < frozen_timestamp)
cond_after_frozen_date = (df['date'] > frozen_date) & (df['created_at'] < df['endOfDayTimestamp'])

# apply the filter
df = df[cond_before_frozen_date | cond_after_frozen_date]

# reduce the DataFrame to only contain rows with the latest 'created_at'
df = df.sort_values('created_at', ascending=False).drop_duplicates('date').sort_index()
'''
