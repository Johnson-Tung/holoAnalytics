import pandas as pd


def live_broadcast_duration(dataframe):
    """Calculates duration of live broadcasts for a YouTube Video Attribute DataFrame.

    Args:
        dataframe: Pandas DataFrame containing actual start and end times of YouTube live broadcasts.

    Returns:
        dataframe: Updated DataFrame with calculated live broadcast durations.
    """

    if 'live_broadcast_duration' not in dataframe:
        dataframe['live_broadcast_duration'] = dataframe['actual_end_time'] - dataframe['actual_start_time']

    return dataframe


def extract_time_values(timedelta, use_days=False):
    """Extracts time values (days, hours, minutes, seconds) from a Pandas Timedelta object.

    Results can be given with or without days, e.g. 1 day, 12 hours versus 36 hours.

    Example: 2 days 03:04:05 = 2 days, 3 hours, 4 minutes, and 5 seconds or 51 hours, 4 minutes, and 5 seconds.

    Args:
        timedelta: Pandas Timedelta object.
        use_days: Boolean specifying if days will be used. If True, days and hours will be given, e.g. 1 day , 12 hours.
                  If False, hours will be given, e.g. 36 hours. Default = False.

    Returns:
        days: Integer specifying the number of days (Optional).
        hours: Integer specifying the number of hours (or total number of hours if use_days is False)
        minutes: Integer specifying the number of minutes.
        seconds: Integer specifying the number of seconds.
    """

    days = timedelta.components.days
    minutes = timedelta.components.minutes
    seconds = timedelta.components.seconds

    if use_days is True:
        hours = timedelta.components.hours
        return days, hours, minutes, seconds
    else:
        hours = timedelta.components.hours + (days * 24)
        return hours, minutes, seconds


def day_of_week(video_attributes):
    """Gets the day of the week for YouTube videos' datetime data, e.g. 'publish_datetime' and 'actual_start_time'.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        video_attributes: Data updated with weekdays for video datetimes.
    """
    datetime_cols = ('publish_datetime', 'actual_start_time', 'actual_end_time')

    for datetime_col in datetime_cols:
        video_attributes[f'{datetime_col}_(weekday)'] = video_attributes[datetime_col].dt.day_name()

    return video_attributes


def check_video_duration(duration, min_length='00:00:00', max_length=None):
    """Checks a YouTube video duration / length if it is between the minimum and maximum values.

    'min_length' and 'max_length' values can be in any format accepted by Pandas Timedelta class. H:M:S format,
    e.g. '00:01:00' for one minute, is recommended.

    Args:
        duration: Pandas Timedelta representing the YouTube video duration.
        min_length: String specifying the minimum video duration.
        max_length: String specifying the maximum video duration.

    Returns:
        Boolean specifying if the YouTube video is within the minimum and maximum lengths.
    """

    if ((max_length is None and duration >= pd.Timedelta(min_length))
            or (max_length is not None and pd.Timedelta(min_length) <= duration <= pd.Timedelta(max_length))):
        return True
    else:
        return False
