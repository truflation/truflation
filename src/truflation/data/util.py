from datetime import datetime

def get_today_string():
    return  datetime.utcnow().date().strftime('%Y-%m-%d')
