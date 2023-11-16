from datetime import datetime
import calendar

def get_base_ym():
    month = datetime.now().month - 1
    year = datetime.now().year
    if month == 0:
        month = 12
        year -= 1
    return datetime(year, month, 1).strftime('%Y%m')

def get_base_date():
    month = datetime.now().month - 1
    year = datetime.now().year
    if month == 0:
        month = 12
        year -= 1
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day).strftime('%Y%m%d')