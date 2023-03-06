from datetime import datetime
import pandas as pd
from pandas.core.dtypes.common import is_timedelta64_dtype

DATETIME_COLS = ['publish_datetime', 'scheduled_start_time', 'scheduled_end_time', 'actual_start_time',
                 'actual_end_time']


def reformat_datetimes(member_channel_data=None, member_video_data=None):
    """Reformats datetime data in YouTube channel and/or video datasets.

    E.g. Convert datetime data from strings to Datetime or Pandas.Timestamp objects.

    Args:
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing YouTube video data,
                           e.g. video stats, for individual Hololive Production members.

    Returns:
        member_channel_data: Updated dictionary containing reformatted YouTube channel data.
        member_video_data: Updated dictionary containing reformatted YouTube video data.
    """

    channel_data_check = isinstance(member_channel_data, dict)
    video_data_check = isinstance(member_video_data, dict)

    if not channel_data_check and not video_data_check:
        return

    if channel_data_check:
        member_channel_data = _reformat_channel_datetimes(member_channel_data)

    if video_data_check:
        for video_data in member_video_data.values():
            if 'video_attributes' in video_data:
                video_data['video_attributes'] = convert_times(video_data['video_attributes'])

    if channel_data_check and video_data_check:
        return member_channel_data, member_video_data
    elif channel_data_check:
        return member_channel_data
    elif video_data_check:
        return member_video_data


def _reformat_channel_datetimes(member_channel_data):

    dataset = 'channel_video_summary'

    if dataset in member_channel_data:
        member_channel_data[dataset] = _reformat_channel_video_summary(member_channel_data[dataset])

    return member_channel_data


def _reformat_channel_video_summary(channel_video_summary):

    for data_column in channel_video_summary['video_attributes']:
        if 'duration' in data_column:
            channel_video_summary = to_timedelta(channel_video_summary, data_column)

    return channel_video_summary


def zulutime_to_utc(date_time, show_tz=False):
    """Converts datetime format from Zulu time to standard UTC.

    Args:
        date_time: String representing a datetime in Zulu time format.
        show_tz: Boolean specifying whether the converted datetime should specify the timezone. Default = False.

    Returns:
        new_date_time: Datetime object representing the input time converted to UTC.
    """

    if isinstance(date_time, (datetime, pd.Timestamp)):
        return date_time

    try:
        new_date_time = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
    except (AttributeError, TypeError):  # E.g. If date_time is 'nan' or invalid ISO format string.
        return pd.NaT
    else:
        if not show_tz:
            new_date_time = new_date_time.replace(tzinfo=None)

    return new_date_time


def timedelta_to_hms(timedelta):
    """Converts Pandas Timedelta to a string using H:M:S format.

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
    """Converts time data in a YouTube Video Attributes Pandas DataFrame into Pandas Timedelta objects.

    Time data includes duration, start times, and end times.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        video_attributes: Updated DataFrame where duration, start times, and end times have been converted to Pandas
                          Timedelta objects.
    """

    video_attributes = to_timedelta(video_attributes, 'duration')

    for datetime_col in DATETIME_COLS:
        video_attributes[datetime_col] = video_attributes[datetime_col].apply(zulutime_to_utc)

    return video_attributes


def to_timedelta(dataframe, column_name):
    """Converts specified DataFrame column containing time duration data into Pandas Timedelta objects.

    Time duration data must be in a format that the 'pandas.to_timedelta' function accepts. If the data is already
    represented by Pandas Timedelta objects, no conversion will be made.

    Args:
        dataframe: Pandas DataFrame.
        column_name: String specifying the column name containing time duration data.

    Returns:
        dataframe: Updated DataFrame where duration data is in the form of Pandas Timedelta objects.
    """

    if not is_timedelta64_dtype(dataframe[column_name]):
        dataframe[column_name] = pd.to_timedelta(dataframe[column_name])

    return dataframe


def combine_same_columns(dataframe, column_name):
    """Combines two "identical" columns that resulted from joining two Pandas DataFrames.

    Example: Combine 'duration_x' and 'duration_y' columns into a single 'duration' column.

    Args:
        dataframe: Pandas DataFrame created after joining two DataFrames.
        column_name: String specifying the columns to be combined.

    Returns:
        dataframe: Original DataFrame but with specified columns combined.
    """

    left_column = f'{column_name}_x'
    right_column = f'{column_name}_y'

    if left_column in dataframe and right_column in dataframe:
        dataframe[column_name] = dataframe.apply(lambda video: _combine_values(video[left_column], video[right_column]),
                                                 axis=1)
        dataframe.drop([left_column, right_column], axis=1, inplace=True)

    return dataframe


def _combine_values(value1, value2):
    """Combines two values and return the result as a string.

     If a value is NaN or missing, it is ignored. If both values are not NaN, combine and separate with a '/'.
     If both values are NaN or missing, return empty string.

    Args:
        value1: First value.
        value2: Second value.

    Returns:
        combined_values: String representing the combined values.
    """
    values = []

    for value in (value1, value2):
        if not pd.isna(value):
            values.append(str(value))

    values.sort()
    combined_values = ' / '.join(values)

    return combined_values


def combine_keyword_banks(*keyword_banks):
    combined_keyword_bank = {}

    # If keyword banks are given as a dictionary of banks instead of individually:
    if len(keyword_banks) == 1 and isinstance(keyword_banks[0], dict):
        keyword_banks = [keyword_bank for keyword_bank in keyword_banks[0].values()]

    keyword_banks = confirm_content_types(keyword_banks)

    for content_type in keyword_banks[0]:
        keywords = []
        for keyword_bank in keyword_banks:
            keywords += keyword_bank[content_type]
        combined_keyword_bank[content_type] = keywords

    return combined_keyword_bank


def confirm_content_types(keyword_banks):
    keyword_banks = list(keyword_banks)

    for index, _ in enumerate(keyword_banks):
        reference = keyword_banks.pop(index)
        keyword_banks = _check_missing_types(reference, keyword_banks)
        keyword_banks.insert(index, reference)

    return keyword_banks


def _check_missing_types(reference, check):
    for content_type in reference:
        for keyword_bank in check:
            if content_type not in keyword_bank:
                keyword_bank[content_type] = []

    return check
