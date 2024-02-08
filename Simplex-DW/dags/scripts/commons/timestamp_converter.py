from datetime import datetime


def convert_to_standard_time(unix_timestamp: int, format_time='%Y-%m-%d %H:%M:%S'):
    return datetime \
        .utcfromtimestamp(unix_timestamp) \
        .strftime(format_time)


def convert_to_unix_time(time: str):
    date = datetime.fromisoformat(time)

    return datetime.timestamp(date)
