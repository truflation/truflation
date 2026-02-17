from datetime import datetime, timezone
import pandas
from truflation.data.export_details import ExportDetails
from truflation.data.logging_manager import Logger
from sqlalchemy import create_engine, types

'''
  Dev Notes
    In general, all databases should have the following fields
    identifiers -- identifiers used in getting identifiers-value pairs
        date -- a date object speficifying the date
        misc identifiers -- like country, position, name, color, source, et cetera
    value -- a value with consistent type, such as double or string
    created_at -- a datetime object indicated when this data was added to the database
'''

def round_value(value, base):
    if value is not None and isinstance(value, float) and value != 0.0:
        base_round = base
        while abs(value) <= 10 ** - base_round:
            base_round += 3

        return round(value, base_round)
    return value

def localize_date(dt: pandas.Series):
    """Convert datetime column to naive format (removes timezone) and ensures proper datetime type."""
    dt = pandas.to_datetime(dt, errors="coerce")  # Convert to datetime first
    if dt.dt.tz is not None:
        dt = dt.dt.tz_localize(None)  # Ensure timezone is removed
    return dt

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
            df_local['created_at'] = pandas.to_datetime(datetime.now(timezone.utc)).tz_localize(None)
        else:
            df_local['created_at'] = localize_date(df_local['created_at'])

        # Read in remote database as dataframe
        df_remote = export_details.read()

        # Reduce future created at to current time
        df_local = self.reduce_future_created_at(df_local)
        df_remote = self.reduce_future_created_at(df_remote)

        # If remote exists, reconcile and receive the data needing to be added
        reconcile = self.reconcile_dataframes if export_details.reconcile is None else export_details.reconcile
        df_new_data = reconcile(df_remote, df_local) if df_remote is not None and not df_remote.empty else df_local
        if not df_new_data.empty:
            self.logging_manager.log_info(
                f'exporting {export_details.name} to {export_details.key}'
            )
            self.logging_manager.log_info(df_new_data)
        else:
            self.logging_manager.log_debug(
                f'no new data - {export_details.name} to {export_details.key}'
            )

        if 'date' in df_local:
            df_local['date'] = localize_date(df_local['date']) # make sure the 'date' column is in datetime format

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
        date_time_now = datetime.now(timezone.utc).replace(tzinfo=None)
        df['created_at'] = localize_date(df['created_at'])
        mask = df['created_at'] > date_time_now
        # Update those rows
        df.loc[mask, 'created_at'] = date_time_now
        return df

    @staticmethod
    def reconcile_dataframes(df_base: pandas.DataFrame, df_incoming: pandas.DataFrame, rounding: int = 6) -> pandas.DataFrame:
        """
        Retrieve a dataframe that contains the rows needed to update df_base with the values from df_incoming.
        Will only skip a row if ALL data columns match (treating NA==NA as equal).
        Compares all columns except identifiers and created_at.
        """

        # If there's no base data, everything incoming is new
        if df_base is None or df_base.empty:
            return df_incoming

        # Reset index if 'date' is used as an index
        if df_incoming.index.name == 'date':
            df_incoming = df_incoming.reset_index()
        if df_base.index.name == 'date':
            df_base = df_base.reset_index()

        # Convert 'date' columns to datetime
        df_base['date'] = localize_date(df_base['date'])
        df_incoming['date'] = localize_date(df_incoming['date'])

        # Make copies to avoid modifying originals
        df_base = df_base.copy()
        df_incoming = df_incoming.copy()

        # Replace empty strings with NA FIRST (before determining column types)
        # This prevents empty string columns from being misclassified
        all_cols = list(df_base.columns)
        for col in all_cols:
            if col in df_base.columns:
                df_base[col] = df_base[col].replace(['', ' ', 'nan', 'None'], pandas.NA)
            if col in df_incoming.columns:
                df_incoming[col] = df_incoming[col].replace(['', ' ', 'nan', 'None'], pandas.NA)

        exclude_from_comparison = ['created_at']
        
        data_cols = []
        for col in all_cols:
            if col in exclude_from_comparison:
                continue
            
            # Try to infer if column is numeric after NA replacement
            try:
                # If column can be converted to numeric (ignoring NAs), it's a data column
                pandas.to_numeric(df_base[col], errors='coerce')
                # Check if at least some non-NA values exist and are numeric
                if pandas.api.types.is_numeric_dtype(df_base[col]) or \
                   df_base[col].dtype in ['Int64', 'Float64', 'float64', 'int64', 'float32', 'int32']:
                    data_cols.append(col)
            except (ValueError, TypeError):
                pass
        
        # Identifier columns: everything that's NOT a data column or excluded
        id_cols = [c for c in all_cols if c not in data_cols and c not in exclude_from_comparison]

        # Keep latest revision per identifier combination
        if 'created_at' in df_base.columns:
            df_base_latest = (
                df_base.sort_values('created_at', ascending=False)
                    .groupby(id_cols, as_index=False, dropna=False)
                    .first()
            )
        else:
            df_base_latest = df_base.drop_duplicates(subset=id_cols, keep='last')

        # Round numeric data columns for consistent comparison
        for col in data_cols:
            if col in df_incoming.columns and pandas.api.types.is_numeric_dtype(df_incoming[col]):
                df_incoming[col] = df_incoming[col].map(lambda x: round_value(x, rounding) if pandas.notna(x) else x)
            if col in df_base_latest.columns and pandas.api.types.is_numeric_dtype(df_base_latest[col]):
                df_base_latest[col] = df_base_latest[col].map(lambda x: round_value(x, rounding) if pandas.notna(x) else x)

        # Convert identifier columns to string for merge (handles NA in identifiers)
        for col in id_cols:
            if col in df_incoming.columns:
                df_incoming[col] = df_incoming[col].astype('string')
            if col in df_base_latest.columns:
                df_base_latest[col] = df_base_latest[col].astype('string')

        # Merge incoming with base data including all data columns
        merge_cols = id_cols + [c for c in data_cols if c in df_base_latest.columns]
        base_for_merge = df_base_latest[merge_cols]

        merged = df_incoming.merge(
            base_for_merge,
            on=id_cols,
            how='left',
            suffixes=('', '_base'),
            indicator=True
        )

        # Check if any data column has changed
        any_column_different = pandas.Series(False, index=merged.index)
        
        for col in data_cols:
            col_base = f'{col}_base'
            if col in merged.columns and col_base in merged.columns:
                # Explicitly handle NA comparisons for this column
                incoming_na = merged[col].isna()
                base_na = merged[col_base].isna()
                
                # Both NA: treat as equal (no difference)
                both_na = incoming_na & base_na
                
                # One NA: treat as different
                one_na = incoming_na ^ base_na
                
                # Both non-NA: compare values
                both_non_na = ~incoming_na & ~base_na
                col_different = pandas.Series(False, index=merged.index)
                col_different[both_non_na] = (
                    merged.loc[both_non_na, col] != merged.loc[both_non_na, col_base]
                )
                
                # Mark as different if one NA or values differ
                any_column_different |= one_na | col_different
        
        # Keep rows where: new data (left_only) OR any column is different
        keep_mask = (merged['_merge'] == 'left_only') | any_column_different

        df_new_data = merged.loc[keep_mask, df_incoming.columns].copy()

        # Ensure ordering and types match base columns where possible
        try:
            df_new_data = df_new_data[df_base.columns]
        except Exception:
            pass

        # Set index to 'date' to match prior behaviour
        if 'date' in df_new_data.columns:
            df_new_data = df_new_data.set_index('date')

        return df_new_data

    # todo -- consider making this take in only a dataframe
    # todo -- review, as this was ChatGPT originated
    def get_frozen_data(self, export_details: ExportDetails, frozen_datetime: datetime = None) -> pandas.DataFrame:
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
        frozen_datetime = datetime.now(timezone.utc) if frozen_datetime is None else frozen_datetime
        frozen_date = frozen_datetime.date()

        df = export_details.read()
        df['date'] = localize_date(df['date'])  # make sure the 'date' column is in datetime format

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