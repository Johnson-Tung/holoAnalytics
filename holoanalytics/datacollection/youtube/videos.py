"""Collection of YouTube video data using the YouTube Data API

This module provides tools to request YouTube video data for specified channels using the YouTube Data API,
extract relevant data from the API's responses, and return the results. By default, copies of the data are exported
to csv files.

Relevant data includes:
    - Video ids
    - Video attributes
    - Video stats

Functions:
    - get_video_data: Retrieves relevant video data of Hololive Production members' videos.
    - get_video_ids: Extracts video ids from API responses.
    - get_video_stats: Extracts video statistics from API responses.
    - get_video_attributes: Extracts video attributes from API responses.
"""

import numpy as np
import pandas as pd
from holoanalytics import definitions as df
from holoanalytics.datacollection.youtube import youtube_api
from holoanalytics.utils import exporting


def get_video_data(client, starting_data, playlist_ids_df, max_results=50, export_data=True):
    """Retrieves relevant video data of Hololive Production members' videos using each members' playlist id.

    Relevant data includes:

    - Video ids
    - Video attributes
    - Video stats

    For each and every member listed in the starting data, this function starts by making requests for their YouTube
    video ids using their playlist id and the YouTube Data API. The video ids are extracted from the API's responses.

    If video ids exist, requests are then made for the remaining types of data specified above, and this data is
    extracted from the API's responses.

    If video ids do not exist (i.e. There are no public videos in the playlist), that member is skipped.

    The function returns a dictionary of dictionaries of Pandas DataFrames where every member with existing video ids
    will have an inner dictionary containing their video data in the form of Pandas DataFrames, one for each of the
    different types of data specific above.

    Unless instructed otherwise, copies of all extracted data are exported.

    Args:
        client: YouTube Data API client.
        starting_data: Starting data file imported as a Pandas DataFrame.
        playlist_ids_df: Pandas Series containing uploads playlist ids for each channel for which video data is
                         being collected for.
        max_results: Integer representing the maximum number of results that the API can return in a single response.
                     Default = 50, the highest possible value.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing video ids, video attributes,
                           and video statistics for each member that has videos in their playlist.

    """
    member_video_data = {}

    id_type = playlist_ids_df.columns[1]  # Get type of playlist id, i.e. uploads or members
    names_and_ids = _link_names_to_ids(starting_data, playlist_ids_df, id_type)

    for member_name, playlist_id in zip(names_and_ids['name'], names_and_ids[id_type]):
        video_data = {}

        playlist_responses = youtube_api.request_data(client, 'playlistItems', ids=playlist_id, max_results=max_results)
        video_data['video_ids'] = get_video_ids(member_name, playlist_responses, export_data)

        if video_data['video_ids'] is None:  # I.e. The API found no public videos in the playlist.
            continue
        else:
            video_data = _video_ids_exist(client, member_name, video_data, max_results, export_data)
            member_video_data[member_name] = video_data

    return member_video_data


def _link_names_to_ids(starting_data, collected_data, id_type):
    """Links the names of Hololive Production members to their respective ids, e.g. playlist ids.

    Args:
        starting_data: Starting data file imported as a Pandas DataFrame.
        collected_data: Pandas DataFrame that the starting data will be merged with.
        id_type: String specifying the type of id (i.e. column) that the DataFrames will be merged on.

    Returns:
        names_and_ids: Pandas DataFrame with 2 columns: member name and id_type.
    """

    names_and_ids = starting_data.merge(collected_data, left_on='youtube_channel_id',
                                        right_on='channel_id')[['name', id_type]]

    return names_and_ids


def _video_ids_exist(client, member_name, video_data, max_results, export_data):
    """Retrieves video attributes and statistics for videos of the current Hololive Production member.

    PRIVATE function.

    Args:
        client:
        all_video_ids: Dictionary of Pandas DataFrames containing video ids, collected so far, of Hololive Production
                       members' videos.
        all_video_attributes: Dictionary of Pandas DataFrames containing video attributes, collected so far, of
                              Hololive Production members' videos.
        all_video_stats: Dictionary of Pandas DataFrames containing video statistics, collected so far, of Hololive
                         Production members' videos.
        member_name: String specifying the name of the Hololive Production member whose videos data is being
                     collected for.
        max_results: Integer representing the maximum number of results that the API can return in a single response.
                     Default = 50, the highest possible value.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        all_video_attributes: Dictionary of Pandas DataFrames updated with video attributes for the current member.
        all_video_stats: Dictionary of Pandas DataFrames updated with video statistics for the current member.
    """

    video_ids = video_data['video_ids']['video_id']

    video_responses = youtube_api.request_data(client, 'video', video_ids, max_results)

    video_data['video_attributes'] = get_video_attributes(member_name, video_responses, export_data)
    video_data['video_stats'] = get_video_stats(member_name, video_responses, export_data)

    return video_data


def get_video_ids(member_name, responses, export_data=True):
    """Extracts video ids for the specified Hololive Production member from the API's responses.

        This function extracts YouTube video ids from YouTube Data API responses, exports a copy of the data
        (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

        Args:
            member_name: String specifying the name of the Hololive Production member whose videos data is being
                         collected for.
            responses: List of YouTube Data API responses to requests for playlistItem data.
            export_data: Boolean specifying whether collected data is to be exported. Default = True.

        Returns:
            data: Pandas DataFrame containing video ids for the current member and date and time each video was added
                  to the playlist.
        """

    video_ids = []
    added_to_playlist = []

    if not responses:
        print(f"{member_name} has no videos available in this playlist.")
        return

    for response in responses:
        results = response['items']
        for result in results:
            video_ids.append(result['snippet']['resourceId']['videoId'])
            # Note: 'publishedAt' refers to the date and time that the video was added to the playlist,
            # not necessarily when the video was published to YouTube.
            added_to_playlist.append(result['snippet']['publishedAt'])

    data = pd.DataFrame(zip(video_ids, added_to_playlist), columns=['video_id', 'added_to_playlist'])

    exporting.export_video_data(member_name, data, export_data, 'uploads_video_ids')

    return data


def get_video_stats(member_name, responses, export_data=True):
    """Extracts video statistics for the specified Hololive Production member's videos from the API's responses.

    This function extracts YouTube video statistics from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

    Args:
        member_name: String specifying the name of the Hololive Production member whose videos data is being
                     collected for.
        responses: List of YouTube Data API responses to requests for video data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing video ids and corresponding video statistics for the current member.
    """

    video_ids = []
    view_counts = []
    like_counts = []
    likes_enabled = []
    comment_counts = []
    comments_enabled = []

    for response in responses:
        results = response['items']
        for result in results:
            video_ids.append(result['id'])
            # 'viewCount' property not available for members-only videos, resulting in KeyErrors
            view_counts.append(result['statistics'].get('viewCount', np.NaN))
            # 'likeCount' property not available if likes are disabled
            like_counts.append(result['statistics'].get('likeCount', np.NaN))
            # Provide additional context in event of likeCount of zero
            likes_enabled.append(('likeCount' in result['statistics']))
            # 'commentCount' property not available if comments are disabled
            comment_counts.append(result['statistics'].get('commentCount', np.NaN))
            # Provide additional context in event of commentCount of zero
            comments_enabled.append(('commentCount' in result['statistics']))

    data = pd.DataFrame(zip(video_ids, view_counts, like_counts, likes_enabled, comment_counts, comments_enabled),
                        columns=['video_id', 'view_count', 'like_count', 'likes_enabled',
                                 'comment_count', 'comments_enabled'])

    exporting.export_video_data(member_name, data, export_data, 'video_stats')

    return data


def get_video_attributes(member_name, responses, export_data=True):
    """Extracts video attributes for the specified Hololive Production member's videos from the API's responses.

    This function extracts YouTube video attributes from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

    Args:
        member_name: String specifying the name of the Hololive Production member whose videos data is being
                     collected for.
        responses: List of YouTube Data API responses to requests for video data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing video ids and corresponding video attributes for the current member.
    """

    video_ids = []
    titles = []
    publish_datetimes = []
    durations = []
    category_ids = []
    is_live_broadcast = []
    scheduled_start_times = []
    scheduled_end_times = []
    actual_start_times = []
    actual_end_times = []

    for response in responses:
        results = response['items']
        for result in results:
            video_ids.append(result['id'])
            titles.append(result['snippet']['title'])
            publish_datetimes.append(result['snippet']['publishedAt'])
            durations.append(result['contentDetails']['duration'])
            category_ids.append(result['snippet']['categoryId'])

            live_broadcast = 'liveStreamingDetails' in result
            is_live_broadcast.append(live_broadcast)
            if live_broadcast:
                scheduled_start_times.append(result['liveStreamingDetails'].get('scheduledStartTime', np.NaN))
                scheduled_end_times.append(result['liveStreamingDetails'].get('scheduledEndTime', np.NaN))
                actual_start_times.append(result['liveStreamingDetails'].get('actualStartTime', np.NaN))
                actual_end_times.append(result['liveStreamingDetails'].get('actualEndTime', np.NaN))
            else:
                scheduled_start_times.append(np.NaN)
                scheduled_end_times.append(np.NaN)
                actual_start_times.append(np.NaN)
                actual_end_times.append(np.NaN)

    data = pd.DataFrame(zip(video_ids, titles, publish_datetimes, durations,
                            category_ids, is_live_broadcast,
                            scheduled_start_times, scheduled_end_times, actual_start_times, actual_end_times),
                        columns=['video_id', 'title', 'publish_datetime', 'duration',
                                 'category_id', 'live_broadcast',
                                 'scheduled_start_time', 'scheduled_end_time', 'actual_start_time', 'actual_end_time'])

    exporting.export_video_data(member_name, data, export_data, 'video_attributes')

    return data
