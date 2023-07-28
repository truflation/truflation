from datetime import datetime


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
