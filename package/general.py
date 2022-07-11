from datetime import datetime


def log_time():
    ymdhm = datetime.now().strftime('%Y-%m-%d %H:%M')
    ymd = datetime.now().strftime('%Y-%m-%d')
    return str(ymdhm)
