import requests
import numpy as np
import pandas as pd
from holoanalytics.datapreparation import calculation as calc
from holoanalytics.datapreparation import reformatting as reform

PREMIERE_COUNTDOWN = pd.Timedelta('00:02:00')
BOUNDS = pd.Timedelta('00:00:15')
PREMIERE_CUTOFF = pd.Timedelta('01:00:01')  # Estimated cutoff before Premieres are reclassified as live streams
LIVE_STREAM_CUTOFF = pd.Timedelta('00:05:00')  # Estimated cutoff before live streams are reclassified as Premieres
SHORT_MAX_LENGTH = pd.Timedelta('00:01:00')


def is_live_stream(video_attributes):
    """Classify YouTube live broadcasts as live streams or Premieres.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        new_video_attributes: Pandas DataFrame containing YouTube video attribute data updated with classifications.

    """

    live_broadcasts = video_attributes[video_attributes['live_broadcast']]

    classified_data = _classify_live_broadcast(live_broadcasts)

    merged_data = video_attributes.merge(classified_data, on='video_id', how='left')
    new_video_attributes = reform.combine_same_columns(merged_data, 'video_type')

    return new_video_attributes


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


def is_short(video_attributes):
    """Classify YouTube non-live broadcasts as shorts or normal / non-short videos.

    Args:
        video_attributes: Pandas DataFrame containing YouTube video attribute data.

    Returns:
        new_video_attributes: Pandas DataFrame containing YouTube video attribute data updated with classifications.

    """

    non_live_broadcasts = video_attributes[~video_attributes['live_broadcast']]

    classified_data = _classify_non_live_broadcast(non_live_broadcasts)

    merged_data = video_attributes.merge(classified_data, on='video_id', how='left')
    new_video_attributes = reform.combine_same_columns(merged_data, 'video_type')

    return new_video_attributes


def _classify_non_live_broadcast(non_live_broadcasts):
    """Classify YouTube videos that are not live broadcasts as Shorts or "normal" videos.

    Args:
        non_live_broadcasts: Pandas DataFrame containing data on YouTube videos that are not live broadcasts.

    Returns:
        classified_data: Pandas DataFrame containing video ids and video types for non-live broadcasts.
    """

    non_live_broadcasts['short_video'] = non_live_broadcasts['duration'] <= SHORT_MAX_LENGTH

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

