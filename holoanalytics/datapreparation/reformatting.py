import datetime
import pandas as pd
from pandas.core.dtypes.common import is_timedelta64_dtype

START_END_TIMES = ['scheduled_start_time', 'scheduled_end_time', 'actual_start_time', 'actual_end_time']


def zulutime_to_utc(date_time, show_tz=False):
    """Convert datetime format from Zulu time to standard UTC.

    Args:
        date_time: String representing a datetime in Zulu time format.
        show_tz: Boolean specifying whether the converted datetime should specify the timezone. Default = False.

    Returns:
        new_date_time: Datetime object representing the input time converted to UTC.
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
    """Convert Pandas Timedelta to a string using H:M:S format.

    Args:
        timedelta: Pandas Timedelta object.

    Returns:
        hms: String specifying the timedelta in H:M:S format.
    """

    days = timedelta.components.days
    hours = timedelta.components.hours + (days * 24)
    minutes = timedelta.components.minutes
    seconds = timedelta.components.seconds

    hms = f'{hours:02}:{minutes:02}:{seconds:02}'

    return hms


def convert_times(video_attributes):
    """Convert duration, start times, and end times in a YouTube Video Attributes DataFrame into Pandas Timedelta objects.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        video_attributes: Updated DataFrame where duration, start times, and end times have been converted to Pandas Timedelta objects.
    """

    video_attributes = to_timedelta(video_attributes, 'duration')

    for time in START_END_TIMES:
        video_attributes[time] = video_attributes[time].apply(zulutime_to_utc)

    return video_attributes


def to_timedelta(dataframe, column_name):
    """Convert a column containing duration data in the form of strings into Pandas Timedelta objects, if not already done.

    Args:
        dataframe: Pandas DataFrame.
        column_name: String specifying the column name containing duration data.

    Returns:
        dataframe: Updated DataFrame where duration data is in the form of Pandas Timedelta objects.
    """

    if not is_timedelta64_dtype(dataframe[column_name]):
        dataframe[column_name] = dataframe[column_name].apply(pd.to_timedelta)

    return dataframe
