import requests
import numpy as np
import pandas as pd
from holoanalytics.datapreparation import calculation as calc
from holoanalytics.datapreparation import reformatting as reform

PREMIERE_COUNTDOWN = pd.Timedelta('00:02:00')
BOUNDS = pd.Timedelta('00:00:15')
PREMIERE_CUTOFF = pd.Timedelta('01:00:01')  # Estimated cutoff before Premieres are reclassified as live streams
LIVE_STREAM_CUTOFF = pd.Timedelta('00:05:00')  # Estimated cutoff before live streams are reclassified as Premieres


def is_live_stream(video_attributes):
    """Classify YouTube live broadcasts as live streams or Premieres.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        new_video_attributes: Pandas DataFrame containing YouTube video attribute data updated with classifications.

    """

    live_broadcasts = check_live_broadcast(video_attributes, True)

    classified_data = _classify_live_broadcast(live_broadcasts)

    merged_data = video_attributes.merge(classified_data, on='video_id', how='left')
    new_video_attributes = combine_same_columns(merged_data, 'video_type')

    return new_video_attributes


def combine_same_columns(dataframe, column_name):
    """Combine two "identical" columns that resulted from joining two Pandas DataFrames.

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
    """Combine two values and return the result as a string.

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


def _classify_live_broadcast(live_broadcasts):
    """Classify YouTube videos that are live broadcasts as live streams or Premieres.

    Args:
        live_broadcasts: Pandas DataFrame containing data on YouTube videos that are live broadcasts.

    Returns:
        classified_data: Pandas DataFrame containing video ids, video types, and live broadcast durations for
                         live broadcasts.
    """

    live_broadcasts = calc.live_broadcast_duration(live_broadcasts)
    live_broadcasts['difference'] = live_broadcasts['live_broadcast_duration'] - live_broadcasts['duration']
    live_broadcasts['video_type'] = np.where(((PREMIERE_COUNTDOWN - BOUNDS) < live_broadcasts['difference'])
                                             & (live_broadcasts['difference'] < (PREMIERE_COUNTDOWN + BOUNDS)),
                                             'Premiere', 'Live Stream')

    # Fix incorrect labels
    live_broadcasts.loc[(live_broadcasts['video_type'] == 'Premiere')
                        & (live_broadcasts['duration'] >= PREMIERE_CUTOFF),
                        'video_type'] = 'Live Stream'
    live_broadcasts.loc[(live_broadcasts['video_type'] == 'Live Stream')
                        & (live_broadcasts['duration'] <= LIVE_STREAM_CUTOFF)
                        & ~(live_broadcasts['live_broadcast_duration'].isna()),
                        'video_type'] = 'Premiere'

    classified_data = live_broadcasts[['video_id', 'live_broadcast_duration', 'video_type']]

    return classified_data


def _check_difference(difference):
    """Check the 'difference' of a YouTube live broadcast and classify the video.

    Args:
        difference: Pandas DataFrame column containing Pandas Timedelta objects that specify the difference
                    between a YouTube live broadcast's live broadcast duration and its video duration.

    Returns:
        video_type: String specifying the video type for the YouTube video associated with the video id.
    """

    if (PREMIERE_COUNTDOWN - BOUNDS) < difference < (PREMIERE_COUNTDOWN + BOUNDS):
        video_type = 'Premiere'
    else:
        video_type = 'Live Stream'

    return video_type


def is_short(video_attributes):
    """Classify YouTube non-live broadcasts as shorts or normal / non-short videos.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        new_video_attributes: Pandas DataFrame containing YouTube video attribute data updated with classifications.

    """

    non_live_broadcasts = check_live_broadcast(video_attributes, False)

    classified_data = _classify_non_live_broadcast(non_live_broadcasts)

    merged_data = video_attributes.merge(classified_data, on='video_id', how='left')
    new_video_attributes = combine_same_columns(merged_data, 'video_type')

    return new_video_attributes


def _classify_non_live_broadcast(non_live_broadcasts):
    """Classify YouTube videos that are not live broadcasts as Shorts or "normal" videos.

    Args:
        non_live_broadcasts: Pandas DataFrame containing data on YouTube videos that are not live broadcasts.

    Returns:
        classified_data: Pandas DataFrame containing video ids and video types for non-live broadcasts.
    """

    non_live_broadcasts['short_video'] = non_live_broadcasts['duration'].apply(check_video_duration,
                                                                               max_length='00:01:00')
    non_live_broadcasts['video_type'] = non_live_broadcasts.apply(
        lambda video: _check_short_long(video['video_id'], video['short_video']), axis=1)
    classified_data = non_live_broadcasts[['video_id', 'video_type']]

    return classified_data


def _check_short_long(video_id, short_video):
    """Check if a YouTube video is "short enough", i.e. one minute length max.

    YouTube Shorts are limited to one minute. All videos that are longer are immediately classified as "normal" videos.
    For videos less than one minute long, check HTTP response status code.

    Args:
        video_id: String representing a YouTube video id.
        short_video: Boolean specifying if a video is less than one minute long.

    Returns:
        video_type: String specifying the video type for the YouTube video associated with the video id.
    """

    if short_video is False:
        video_type = 'Normal'
    else:
        video_type = _check_response_code(video_id)

    return video_type


def _check_response_code(video_id):
    """Access YouTube's website, get the HTTP response status code, and classify a YouTube video based on the code.

    How it works:

    Access 'https://www.youtube.com/shorts/video_id'. This URL only works for shorts and will return a code of 200
    for shorts. Non-shorts / "normal videos" will redirect to 'https://www.youtube.com/watch?v=video_id', which works
    for all YouTube videos and will return a code of 303.

    Args:
        video_id: String representing a YouTube video id.

    Returns:
        video_type: String specifying the video type for the YouTube video associated with the video id.
    """

    response = requests.head(f'https://www.youtube.com/shorts/{video_id}', verify=True)
    code = response.status_code

    if code == 200:
        video_type = 'Short'
    elif code == 303:
        video_type = 'Normal'
    else:
        video_type = None

    return video_type


def check_live_broadcast(video_attributes, is_live_broadcast):
    """Check for YouTube videos that are live broadcasts and return the data for those videos only.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.
        is_live_broadcast: Pandas DataFrame column containing Booleans that specify if a YouTube video is a
                           live broadcast.

    Returns:
        dataframe: Subset of video_attributes that only contains live broadcasts.
    """

    dataframe = video_attributes[video_attributes['live_broadcast'] == is_live_broadcast]

    return dataframe


def filter_video_duration(video_attributes, min_length='00:00:00', max_length=None):
    """Filter a YouTube Video Attributes DataFrame to remove all videos whose duration is not within the minimum and
    maximum lengths.

    'min_length' and 'max_length' values can be in any format accepted by Pandas Timedelta class. H:M:S format,
    e.g. '00:01:00' for one minute, is recommended.

    Args:
        video_attributes: Pandas DataFrame containing attributes of YouTube videos.
        min_length: String specifying the minimum video duration accepted. Videos whose duration is below this value
                    are filtered out. Default = '00:00:00'.
        max_length: String specifying the maximum video duration accepted. Videos whose duration is above this value
                    are filtered out. Default = None.

    Returns:
        filtered_data: Filtered DataFrame containing attributes of YouTube videos whose durations are within the
                       minimum and maximum boundaries.
    """

    video_attributes = reform.to_timedelta(video_attributes, 'duration')

    if max_length is None:
        filtered_data = video_attributes[(video_attributes['duration'] >= pd.Timedelta(min_length))]
    else:
        filtered_data = video_attributes[(pd.Timedelta(min_length) <= video_attributes['duration']
                                          <= pd.Timedelta(max_length))]

    return filtered_data


def check_video_duration(duration, min_length='00:00:00', max_length=None):
    """Check a YouTube video duration / length if it is between the minimum and maximum values.

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
