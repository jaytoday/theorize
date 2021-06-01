from typing import Union
from datetime import datetime

def datetime_to_unix_timestamp(
        date_string: str,
        datetime_format: str = "%Y-%m-%d %H:%M:%S"
) -> str:
    """
    Helper method to convert datetime to unix timestamp

    :param str date_string: date in string format
    :param str datetime_format: format of the input date.
        Defaults to "%Y-%m-%d %H:%M:%S"
    :return str: unix timestamp of the given date
    """
    date_time = datetime.strptime(date_string, datetime_format)
    return str(int(date_time.timestamp()))


def unix_timestamp_to_datetime_str(
        timestamp: Union[int, str],
        datetime_format: str = "%Y-%m-%d %H:%M:%S"
) -> str:
    """
    Helper method to convert datetime to unix timestamp

    :param str date_string: date in string format
    :param str datetime_format: format of the input date.
        Defaults to "%Y-%m-%d %H:%M:%S"
    :return str: unix timestamp of the given date
    """
    if not isinstance(timestamp, int):
        timestamp = int(timestamp)
    date_time = datetime.fromtimestamp(timestamp)
    return date_time.strftime(datetime_format)
