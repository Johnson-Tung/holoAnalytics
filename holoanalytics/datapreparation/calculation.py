from holoanalytics.datapreparation import reformatting as reform


def live_broadcast_duration(dataframe):
    """Calculate duration of live broadcasts for a YouTube Video Attribute DataFrame.

    Args:
        dataframe: Pandas DataFrame containing actual start and end times of YouTube live broadcasts.

    Returns:
        dataframe: Updated DataFrame with calculated live broadcast durations.
    """

    if 'live_broadcast_duration' not in dataframe:
        dataframe['live_broadcast_duration'] = dataframe['actual_end_time'] - dataframe['actual_start_time']
    return dataframe


def extract_time_values(timedelta, use_days=False):
    """Extract time values (days, hours, minutes, seconds) from a Pandas Timedelta object.

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
    """Get the day of the week for YouTube videos' datetime data, e.g. 'actual_start_time'.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        video_attributes: Data updated with weekdays for video datetimes.
    """

    for time in reform.START_END_TIMES:
        video_attributes[f'{time}_(weekday)'] = video_attributes[time].dt.day_name()

    return video_attributes
