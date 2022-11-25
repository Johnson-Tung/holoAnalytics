import requests
import numpy as np
import pandas as pd
from holoanalytics.datapreparation import calculation as calc
from holoanalytics.datapreparation import reformatting as reform
from holoanalytics.utils import exporting

PREMIERE_COUNTDOWN = pd.Timedelta('00:02:00')  # Default countdown length for Premieres, as decided by YouTube
BOUNDS = pd.Timedelta('00:00:15')  # Estimated max deviation from PREMIERE_COUNTDOWN, i.e. 2 minutes +/- 15 seconds
PREMIERE_CUTOFF = pd.Timedelta('01:00:01')  # Estimated cutoff before Premieres are reclassified as live streams
LIVE_STREAM_CUTOFF = pd.Timedelta('00:05:00')  # Estimated cutoff before live streams are reclassified as Premieres
SHORT_MAX_LENGTH = pd.Timedelta('00:01:00')  # Maximum length for a short, as decided by YouTube


def classify_video_type(member_video_data, export_data=True):
    """Determines the video type for the YouTube videos in the imported data.

    All videos will have one of four types:
    1) Normal
    2) Short
    3) Live Stream
    4) Premiere

    Args:
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing YouTube video data,
                           originally returned by holoanalytics.utils.importing.import_video_data().
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        member_video_data: YouTube video data updated with video type data.
    """

    for member_name, video_data in member_video_data.items():
        video_data['video_type'] = _classify_member_videos(member_name, video_data['video_attributes'], export_data)

    return member_video_data


def _classify_member_videos(member_name, video_attributes, export_data):
    """Determines the video type for the specified Hololive Production member's YouTube videos.

    Args:
        member_name: String specifying the name of the Hololive Production member whose video data is being
                     prepared.
        video_attributes: Pandas DataFrame containing video attribute data for the specified member.
        export_data: Boolean specifying whether collected data is to be exported.

    Returns:
        data: Pandas DataFrame containing video ids and video type for the member's videos.
    """

    video_attributes = is_live_stream(video_attributes)
    video_attributes = is_short(video_attributes)

    data = video_attributes[['video_id', 'video_type']]

    exporting.export_video_data(member_name, data, export_data, 'video_types')

    return data


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

    non_live_broadcasts['video_type'] = np.where(~(non_live_broadcasts['duration'] <= SHORT_MAX_LENGTH),
                                                 'Normal', 'Unknown')
    mask = non_live_broadcasts['video_type'] == 'Unknown'
    non_live_broadcasts.loc[mask, 'video_type'] = non_live_broadcasts.loc[mask, 'video_id'].apply(_check_response_code)

    classified_data = non_live_broadcasts[['video_id', 'video_type']]

    return classified_data


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

