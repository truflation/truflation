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


# todo -- add in ability to create custom restriction, like a number x: 0<=x<=100
def clean_date_value_dfs(df: pd.DataFrame, value_dtype: str = 'number') -> pd.DataFrame:
    """
    Removes all rows that have invalid dates or non-numeric values. Sets index to 'date'.

    :param df: Input DataFrame.
    :param value_dtype: Data type of the 'value' column. Default is 'number'.
    :return: Cleaned DataFrame.
    """

    # Convert 'date' column to datetime, and set errors='coerce' to handle invalid dates
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Process 'value' column based on its data type
    if value_dtype == 'number':
        # Convert 'value' to numeric and replace non-numeric values with pd.NA
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
    elif value_dtype == 'string':
        # Convert 'value' to string and replace empty strings with pd.NA
        df['value'] = df['value'].astype(str).replace('', pd.NA)
    elif value_dtype == 'boolean':
        # Convert 'value' to boolean; invalid entries will default to False
        df['value'] = df['value'].astype('bool')
    elif value_dtype == 'datetime':
        # Convert 'value' to datetime and replace invalid dates with pd.NaT
        df['value'] = pd.to_datetime(df['value'], errors='coerce')
    elif value_dtype == 'category':
        # Convert 'value' to category; invalid entries will be treated as pd.NA
        df['value'] = df['value'].astype('category')
    elif value_dtype == 'integer':
        # Convert 'value' to integer and replace non-integer values with pd.NA
        df['value'] = pd.to_numeric(df['value'], errors='coerce', downcast='integer')
    elif value_dtype == 'float':
        # Convert 'value' to float and replace non-float values with pd.NA
        df['value'] = pd.to_numeric(df['value'], errors='coerce', downcast='float')
    # Add more conditions for other data types if needed

    # Remove rows where 'date' column has pd.NaT (due to invalid dates) and 'value' is pd.NA
    df = df.dropna(subset=['date', 'value'])

    # Set 'date' column as the index
    df = df.set_index('date')

    return df
