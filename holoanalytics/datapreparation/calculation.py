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
    days = timedelta.components.days
    minutes = timedelta.components.minutes
    seconds = timedelta.components.seconds

    if use_days is True:
        hours = timedelta.components.hours
        return days, hours, minutes, seconds
    else:
        hours = timedelta.components.hours + (days * 24)
        return hours, minutes, seconds


def weekdays(video_attributes):  # Consider ignoring times except for actual start time

    for time in reform.START_END_TIMES:
        video_attributes[f'{time}_(weekday)'] = video_attributes[time].dt.day_name()

    return video_attributes
