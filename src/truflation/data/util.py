from datetime import datetime


def get_today_string():
    """ Returns todays' date in format year-month-day; Example:2023-06-23 """
    return datetime.utcnow().date().strftime('%Y-%m-%d')
