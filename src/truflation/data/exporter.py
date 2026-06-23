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
        elif not dry_run and not export_details.replace and isinstance(df_local, pandas.DataFrame):
            # No new data, but still notify the connector so batch writers can finalize
            export_details.write(df_new_data)

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
        
        # Store original dtypes from incoming (before any conversions)
        # This is the "source of truth" for column types
        incoming_original_dtypes = {col: df_incoming[col].dtype for col in df_incoming.columns}

        # Convert pyarrow dtypes to regular pandas dtypes for consistent comparison
        # PyArrow types can cause merge and comparison issues
        for col in df_base.columns:
            dtype_name = str(df_base[col].dtype)
            if 'pyarrow' in dtype_name or 'Arrow' in dtype_name:
                if 'string' in dtype_name:
                    df_base[col] = df_base[col].astype('object')
                elif 'double' in dtype_name or 'float' in dtype_name:
                    df_base[col] = df_base[col].astype('float64')
                elif 'int' in dtype_name:
                    df_base[col] = df_base[col].astype('Int64')  # Nullable integer
                elif 'timestamp' in dtype_name:
                    df_base[col] = pandas.to_datetime(df_base[col])
                    
        for col in df_incoming.columns:
            dtype_name = str(df_incoming[col].dtype)
            if 'pyarrow' in dtype_name or 'Arrow' in dtype_name:
                if 'string' in dtype_name:
                    df_incoming[col] = df_incoming[col].astype('object')
                elif 'double' in dtype_name or 'float' in dtype_name:
                    df_incoming[col] = df_incoming[col].astype('float64')
                elif 'int' in dtype_name:
                    df_incoming[col] = df_incoming[col].astype('Int64')  # Nullable integer
                elif 'timestamp' in dtype_name:
                    df_incoming[col] = pandas.to_datetime(df_incoming[col])

        # Standardize NA values - but handle identifier vs data columns differently
        # For string identifier columns: keep as empty string '' for consistency
        # For data columns: convert to pandas.NA
        all_cols = list(df_base.columns)

        exclude_from_comparison = ['created_at']
        
        data_cols = []
        id_cols = []
        
        for col in all_cols:
            if col in exclude_from_comparison:
                continue
            
            # Use original incoming dtype as source of truth
            # This avoids issues where DB columns with all NA values have wrong dtype
            original_dtype = incoming_original_dtypes.get(col)
            
            # Datetime/timestamp columns are always identifiers
            is_datetime = pandas.api.types.is_datetime64_any_dtype(df_base[col])
            if original_dtype and pandas.api.types.is_datetime64_any_dtype(original_dtype):
                is_datetime = True
            
            if is_datetime:
                id_cols.append(col)
                continue
            
            # Check if column is numeric based on original incoming dtype
            is_numeric = False
            if original_dtype:
                is_numeric = pandas.api.types.is_numeric_dtype(original_dtype)
            else:
                # Column not in incoming, check base
                is_numeric = pandas.api.types.is_numeric_dtype(df_base[col])
            
            if is_numeric:
                # Numeric column -> data column
                data_cols.append(col)
                continue
            
            # String/object columns - check if they contain numeric data
            has_numeric = False
            if pandas.api.types.is_string_dtype(df_base[col]) or pandas.api.types.is_object_dtype(df_base[col]):
                try:
                    numeric_test = pandas.to_numeric(df_base[col], errors='coerce')
                    has_numeric = numeric_test.notna().any()
                except (ValueError, TypeError):
                    pass

            # Also check the incoming column for numeric content.
            # This covers cases where the base has an exotic dtype (e.g. decimal128[pyarrow])
            # that doesn't pass is_string/object_dtype, but the incoming column is a
            # string/object column whose values are actually numeric.
            if not has_numeric and col in df_incoming.columns:
                if original_dtype and (
                    pandas.api.types.is_string_dtype(original_dtype) or
                    pandas.api.types.is_object_dtype(original_dtype)
                ):
                    try:
                        numeric_test = pandas.to_numeric(df_incoming[col], errors='coerce')
                        has_numeric = numeric_test.notna().any()
                    except (ValueError, TypeError):
                        pass

            if has_numeric:
                data_cols.append(col)
            elif pandas.api.types.is_string_dtype(df_base[col]) or pandas.api.types.is_object_dtype(df_base[col]) or (
                original_dtype and (
                    pandas.api.types.is_string_dtype(original_dtype) or
                    pandas.api.types.is_object_dtype(original_dtype)
                )
            ):
                # String column with no numeric values -> identifier
                id_cols.append(col)
            else:
                # Default: treat as identifier
                id_cols.append(col)
        
        # Safety check: must have at least one identifier column
        if not id_cols:
            raise ValueError(f"No identifier columns found! all_cols={all_cols}, data_cols={data_cols}, dtypes={df_base.dtypes.to_dict()}")
        
        # Standardize NA values based on column type
        # For string identifier columns: NULL/None -> '' (empty string) for consistency
        # For numeric data columns: empty string -> pandas.NA
        for col in id_cols:
            if col in df_base.columns:
                # Convert NULL/None to empty string in identifier columns
                df_base[col] = df_base[col].fillna('').replace(['nan', 'None', 'null'], '')
            if col in df_incoming.columns:
                df_incoming[col] = df_incoming[col].fillna('').replace(['nan', 'None', 'null'], '')
        
        for col in data_cols:
            if col in df_base.columns:
                # Convert empty strings to pandas.NA in data columns
                df_base[col] = df_base[col].replace(['', ' ', 'nan', 'None', 'null'], pandas.NA)
            if col in df_incoming.columns:
                df_incoming[col] = df_incoming[col].replace(['', ' ', 'nan', 'None', 'null'], pandas.NA)

        # Deduplicate incoming by identifiers + data columns
        # Keep latest per UNIQUE combination of identifiers AND data values
        dedup_cols = id_cols + data_cols
        if 'created_at' in df_incoming.columns:
            df_incoming_latest = (
                df_incoming.sort_values('created_at', ascending=False)
                    .groupby(dedup_cols, as_index=False, dropna=False)
                    .first()
            )
        else:
            df_incoming_latest = df_incoming.drop_duplicates(subset=dedup_cols, keep='last')
        df_incoming = df_incoming_latest

        # Normalize data columns to float64 and round for consistent comparison.
        # hash_pandas_object treats Python int and float as distinct types even for
        # numerically equal values (int(4635) != float(4635.0) in the hash), so a
        # DB float64 column would never match an incoming int64 column without this
        # cast. to_numeric also handles object columns containing Decimal values.
        for col in data_cols:
            if col in df_incoming.columns:
                df_incoming[col] = pandas.to_numeric(df_incoming[col], errors='coerce').astype('float64')
                df_incoming[col] = df_incoming[col].map(lambda x: round_value(x, rounding) if pandas.notna(x) else x)
            if col in df_base.columns:
                df_base[col] = pandas.to_numeric(df_base[col], errors='coerce').astype('float64')
                df_base[col] = df_base[col].map(lambda x: round_value(x, rounding) if pandas.notna(x) else x)

        # Only insert rows that do not already exist with the same identifiers and data values
        compare_cols = [col for col in (id_cols + data_cols) if col in df_incoming.columns and col in df_base.columns]
        if not compare_cols:
            return df_incoming

        # NOTE: do not use pandas.util.hash_pandas_object here. Its categorize=True
        # codepath (factorize-based hashing of object-dtype columns) has been proven
        # to return a different hash for the *same* value depending on what else is
        # in the array at scale (confirmed on real data: the same 'date' value hashed
        # two different ways depending on whether it was row 668055 or 668056 of an
        # otherwise-identical array). That silently breaks dedup on multi-million-row
        # tables. A merge-based exact-key join does not have this failure mode.
        def build_compare_key(df: pandas.DataFrame, cols: list[str]) -> pandas.DataFrame:
            temp = df[cols].copy()
            for col in cols:
                temp[col] = temp[col].astype('object')
                temp[col] = temp[col].where(~temp[col].isna(), '__TRUFLATION_NA__')
            return temp

        base_keys = build_compare_key(df_base, compare_cols).drop_duplicates()
        incoming_keys = build_compare_key(df_incoming, compare_cols)
        merged = incoming_keys.merge(base_keys, on=compare_cols, how='left', indicator=True)
        keep_mask = (merged['_merge'] == 'left_only').to_numpy()

        df_new_data = df_incoming.iloc[keep_mask].copy()

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