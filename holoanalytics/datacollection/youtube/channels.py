"""Collection of YouTube channel data using the YouTube Data API

This module provides tools to request YouTube channel data for specified channels using the YouTube Data API,
extract relevant data from the API's responses, and return the results. By default, copies of the data are exported
to csv files.

Relevant data includes:
    - Channel titles
    - Channel stats
    - Channel thumbnail URLs
    - Uploads playlist ids

Functions:
    - get_channel_data: Retrieves relevant channel data of Hololive Production members' channels.
    - get_channel_titles: Extracts channel titles from API responses.
    - get_channel_stats: Extracts channel statistics from API responses.
    - get_channel_thumbnail_urls: Extracts channel thumbnail URLs from API responses.
"""

import pandas as pd
from holoanalytics import definitions as df
from holoanalytics.datacollection.youtube import youtube_api
from holoanalytics.utils import exporting


def get_channel_data(client, channel_ids, max_results=50, export_data=True):
    """Retrieves relevant channel data of Hololive Production members' channels using each members' channel id.

    Relevant data includes:
    - Channel titles
    - Channel stats
    - Channel thumbnail URLs
    - Uploads playlist ids

    This function makes requests for YouTube channel data using the YouTube Data API, extracts relevant data from the
    API's responses to the request, exports copies of the data (unless instructed otherwise), and returns Pandas
    DataFrames containing the collected data.

    Args:
        client: YouTube Data API client.
        channel_ids: Sequence, e.g. Pandas Series, of strings representing YouTube channel ids.
        max_results: Integer representing the maximum number of results that the API can return in a single response.
                     Default = 50, the highest possible value.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        channel_titles: Pandas DataFrame containing channel ids and corresponding channel titles.
        channel_stats: Pandas DataFrame containing channel ids and corresponding channel statistics.
        channel_thumbnail_urls: Pandas DataFrame containing channel ids and corresponding thumbnail URLs
        uploads_playlist_ids: Pandas DataFrame containing channel ids and corresponding uploads playlist ids.
    """
    member_channel_data = {}

    if export_data is True and df.SESSION_PATH is not None:
        _ = exporting.create_directory(df.SESSION_PATH, 'Channel', add_date=False)

    responses = youtube_api.request_data(client, 'channel', channel_ids, max_results)

    member_channel_data['channel_titles'] = get_channel_titles(responses, export_data)
    member_channel_data['channel_stats'] = get_channel_stats(responses, export_data)
    member_channel_data['channel_thumbnail_urls'] = get_channel_thumbnail_urls(responses, export_data)
    member_channel_data['uploads_playlist_ids'] = get_uploads_playlist_ids(responses, export_data)

    return member_channel_data


def get_channel_titles(responses, export_data=True):
    """Extracts channel titles for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube channel titles from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

    Args:
        responses: List containing the API's responses to requests for YouTube channel data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing channel ids and corresponding channel titles
    """

    channel_ids = []
    channel_titles = []

    for response in responses:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            channel_titles.append(result['snippet']['title'])

    data = pd.DataFrame(zip(channel_ids, channel_titles), columns=['channel_id', 'channel_title'])

    exporting.export_channel_data(data, export_data, 'channel_titles')

    return data


def get_channel_stats(responses, export_data=True):
    """Extracts channel statistics for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube channel statistics from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

    Channel statistics include:
    - Subscriber counts
    - Video counts
    - Total view counts

    Args:
        responses: List containing the API's responses to requests for YouTube channel data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing channel ids and corresponding channel statistics.
    """

    channel_ids = []
    subscriber_counts = []
    video_counts = []
    view_counts = []

    for response in responses:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            subscriber_counts.append(int(result['statistics']['subscriberCount']))
            video_counts.append(int(result['statistics']['videoCount']))
            view_counts.append(int(result['statistics']['viewCount']))

    data = pd.DataFrame(zip(channel_ids, subscriber_counts, video_counts, view_counts),
                        columns=['channel_id', 'subscriber_count', 'video_count', 'view_count'])

    exporting.export_channel_data(data, export_data, 'channel_stats')

    return data


def get_channel_thumbnail_urls(responses, export_data=True):
    """Extracts channel thumbnail URLs for the specified Hololive Production member's channel from the API's responses.

    This functions extracts YouTube channel thumbnail (profile picture) URLs from YouTube Data API responses,
    exports a copy of the data (unless instructed otherwise), and returns a Pandas DataFrame containing the collected
    data. Up to three URLs will be returned per channel for different resolutions, i.e. default, medium, and high.
    URLs can then be used to download the images.

    Args:
        responses: List containing the API's responses to requests for YouTube channel data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing channel ids and corresponding thumbnail URLs.
    """

    channel_ids = []
    default_thumbnail_urls = []
    medium_thumbnail_urls = []
    high_thumbnail_urls = []

    for response in responses:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            default_thumbnail_urls.append(result['snippet']['thumbnails']['default']['url'])
            medium_thumbnail_urls.append(result['snippet']['thumbnails']['medium']['url'])
            high_thumbnail_urls.append(result['snippet']['thumbnails']['high']['url'])

    data = pd.DataFrame(zip(channel_ids, default_thumbnail_urls, medium_thumbnail_urls, high_thumbnail_urls),
                        columns=['channel_id', 'default', 'medium', 'high'])

    exporting.export_channel_data(data, export_data, 'channel_thumbnail_urls')

    return data


def get_uploads_playlist_ids(responses, export_data=True):
    """Extracts uploads playlist ids for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube uploads playlist ids from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns a Pandas DataFrame containing the collected data.

    Args:
        responses: List containing the API's responses to requests for YouTube channel data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        data: Pandas DataFrame containing channel ids and corresponding uploads playlist ids.
    """

    channel_ids = []
    uploads_playlist_ids = []

    for response in responses:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            uploads_playlist_ids.append(result['contentDetails']['relatedPlaylists']['uploads'])

    data = pd.DataFrame(zip(channel_ids, uploads_playlist_ids), columns=['channel_id', 'uploads_playlist_id'])

    exporting.export_channel_data(data, export_data, 'uploads_playlist_ids')

    return data
