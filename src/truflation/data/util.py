from datetime import datetime
import pandas as pd

def get_today_string():
    """ Returns todays' date in format year-month-day; Example:2023-06-23 """
    return datetime.utcnow().date().strftime('%Y-%m-%d')


# def format_duration(seconds):
#     # Constants for time conversion
#     MINUTE = 60
#     HOUR = 60 * MINUTE
#     DAY = 24 * HOUR
#
#     # Calculate days, hours, minutes, and seconds
#     days, seconds = divmod(seconds, DAY)
#     hours, seconds = divmod(seconds, HOUR)
#     minutes, seconds = divmod(seconds, MINUTE)
#
#     # flatten seconds
#     seconds = int(seconds)
#
#     # Format the duration string
#     duration_str = ""
#     if days > 0:
#         duration_str += f"{days} days, "
#     if hours > 0:
#         duration_str += f"{hours} hours, "
#     if minutes > 0:
#         duration_str += f"{minutes} minutes, "
#     duration_str += f"{seconds} seconds"
#
#     return duration_str
#


def format_duration(seconds):
    # Constants for time conversion
    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    # Calculate days, hours, minutes, and seconds
    days, seconds = divmod(seconds, DAY)
    hours, seconds = divmod(seconds, HOUR)
    minutes, seconds = divmod(seconds, MINUTE)

    # Format the duration string
    duration_str = ""
    if days > 0:
        duration_str += f"{days} days, "
    if hours > 0:
        duration_str += f"{hours} hours, "
    if minutes > 0:
        duration_str += f"{minutes} minutes, "
    if seconds > 0:
        sec_str = f"{seconds:.2f}"
        sec_str = sec_str.rstrip('0')
        if sec_str.endswith('.'):
            sec_str = sec_str[:-1]
        duration_str += f"{sec_str} seconds"

    # Remove trailing zeros and unnecessary decimal point
    duration_str = duration_str.rstrip('0')
    if duration_str.endswith('.'):
        duration_str = duration_str[:-1]

    return duration_str


def safe_apply(func: callable, value) -> bool:
    """
    Safely apply a function to a value. If the function raises an exception, return False.

    :param func: Function to apply.
    :param value: Value to apply the function on.
    :return: Result of the function or False if an exception occurs.
    """
    try:
        return func(value)
    except Exception:
        return False


def clean_date_value_dfs(df: pd.DataFrame, value_dtype: str = 'number', restriction_fn: callable = None, column_name: str = 'value') -> pd.DataFrame:
    """
    Removes all rows that have invalid dates or non-numeric values. Sets index to 'date'.

    :param df: Input DataFrame.
    :param value_dtype: Data type of the target column. Default is 'number'.
    :param restriction_fn: A function that checks the validity of each data point in the target column.
                           It should return True if the value is valid and False otherwise.
    :param column_name: Name of the column to be cleaned. Default is 'value'.
    :return: Cleaned DataFrame.
    """

    # Convert 'date' column to datetime, and set errors='coerce' to handle invalid dates
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Process 'value' column based on its data type
    if value_dtype == 'number':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    elif value_dtype == 'string':
        df[column_name] = df[column_name].astype(str).replace('', pd.NA)
    elif value_dtype == 'boolean':
        df[column_name] = df[column_name].astype('bool')
    elif value_dtype == 'datetime':
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    elif value_dtype == 'category':
        df[column_name] = df[column_name].astype('category')
    elif value_dtype == 'integer':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce', downcast='integer')
    elif value_dtype == 'float':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce', downcast='float')
    # Add more conditions for other data types if needed

    # Apply the custom restriction function to the 'value' column if provided
    if restriction_fn:
        df = df[df[column_name].apply(lambda x: safe_apply(restriction_fn, x))]

    # Remove rows where 'date' column has pd.NaT (due to invalid dates) and 'value' is pd.NA
    df = df.dropna(subset=['date', 'value'])

    # Set 'date' column as the index
    df = df.set_index('date')

    return df
