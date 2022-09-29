import datetime
import pandas as pd
from pandas.core.dtypes.common import is_timedelta64_dtype

TIMES = ['scheduled_start_time', 'scheduled_end_time', 'actual_start_time', 'actual_end_time']


def convert_duration_to_hms(duration):
    timedelta = pd.Timedelta(duration)
    days = timedelta.components.days
    hours = timedelta.components.hours + days*24
    minutes = timedelta.components.minutes
    seconds = timedelta.components.seconds
    hms = f'{hours}:{minutes}:{seconds}'
    return hms


def zulutime_to_utc(date_time, show_tz=False):
    """Convert datetime format from Zulu time to standard UTC.

    Args:
        date_time: String representing a datetime in Zulu time format.
        show_tz: Boolean specifying whether the converted datetime should specify the timezone. Default = False.

    Returns:
        new_date_time: String representing the datetime converted to UTC.
    """

    try:
        new_date_time = datetime.datetime.fromisoformat(date_time.replace('Z', '+00:00'))
    except (AttributeError, ValueError):  # E.g. If date_time is 'nan' or invalid isoformat string.
        return pd.NaT
    else:
        if not show_tz:
            new_date_time = new_date_time.replace(tzinfo=None)

    return new_date_time


def timedelta_to_hms(timedelta):

    days = timedelta.components.days
    hours = timedelta.components.hours + (days * 24)
    minutes = timedelta.components.minutes
    seconds = timedelta.components.seconds

    hms = f'{hours:02}:{minutes:02}:{seconds:02}'

    return hms


def convert_times(video_attributes):

    video_attributes = to_timedelta(video_attributes, 'duration')
    video_attributes[TIMES] = video_attributes[TIMES].apply(pd.to_timedelta)

    return video_attributes


def to_timedelta(dataframe, column_name):

    if not is_timedelta64_dtype(dataframe[column_name]):
        dataframe[column_name] = dataframe[column_name].apply(pd.to_timedelta)

    return dataframe
