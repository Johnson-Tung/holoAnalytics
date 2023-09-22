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
from holoanalytics.settings import session
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
    API's responses to the requests, exports copies of the data (unless instructed otherwise), and returns the data
    along with their timestamps.

    Args:
        client: YouTube Data API client.
        channel_ids: Sequence, e.g. Pandas Series, of strings representing YouTube channel ids.
        max_results: Integer representing the maximum number of results that the API can return in a single response.
                     Default = 50, the highest possible value.
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        member_channel_data: Two-level dictionary where:
                             Level 1 contains:
                                a) Keys that are strings specifying the data type, e.g. 'channel_stats'.
                                b) Values that are dictionaries containing the data and timestamps.
                             Level 2 contains two key-value pairs:
                                a) 'data': Pandas DataFrame containing channel ids and their corresponding
                                           channel titles, channel statistics, thumbnail URLs, or uploads playlist ids.
                                b) 'datetime': Datetime object specifying the date and time when the data was returned
                                               by the API.
    """
    member_channel_data = {}

    if export_data is True and session.SESSION_PATH is not None:
        _ = exporting.create_directory(session.SESSION_PATH, 'Channel', add_date=False)

    responses = youtube_api.request_data(client, 'channel', channel_ids, max_results)

    member_channel_data['channel_titles'] = get_channel_titles(responses, export_data)
    member_channel_data['channel_stats'] = get_channel_stats(responses, export_data)
    member_channel_data['channel_thumbnail_urls'] = get_channel_thumbnail_urls(responses, export_data)
    member_channel_data['uploads_playlist_ids'] = get_uploads_playlist_ids(responses, export_data)

    return member_channel_data


def get_channel_titles(responses, export_data=True):
    """Extracts channel titles for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube channel titles from YouTube Data API responses, exports a copy of the data (unless
    instructed otherwise), and returns the data along with its timestamp.

    Args:
        responses: Dictionary containing the YouTube Data API's responses to requests for YouTube channel data and a
                   timestamp, returned by youtube_api.request_data().
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        channel_data: Dictionary containing two key-value pairs:
                      1) 'data': Pandas DataFrame containing channel ids and their corresponding channel titles
                      2) 'datetime': Datetime object specifying the date and time when the data was returned by the API.
    """
    channel_data = {}
    channel_ids = []
    channel_titles = []

    for response in responses['responses']:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            channel_titles.append(result['snippet']['title'])

    channel_data['data'] = pd.DataFrame(zip(channel_ids, channel_titles),
                                        columns=['channel_id', 'channel_title'])
    channel_data['datetime'] = responses['datetime']

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'channel_titles', channel_data['datetime'])

    return channel_data


def get_channel_stats(responses, export_data=True):
    """Extracts channel statistics for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube channel statistics from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns the data along with its timestamp.

    Channel statistics include:
    - Subscriber counts
    - Video counts
    - Total view counts

    Args:
        responses: Dictionary containing the YouTube Data API's responses to requests for YouTube channel data and a
                   timestamp, returned by youtube_api.request_data().
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        channel_data: Dictionary containing two key-value pairs:
                      1) 'data': Pandas DataFrame containing channel ids and their corresponding channel statistics.
                      2) 'datetime': Datetime object specifying the date and time when the data was returned by the API.
    """
    channel_data = {}
    channel_ids = []
    subscriber_counts = []
    video_counts = []
    view_counts = []

    for response in responses['responses']:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            subscriber_counts.append(int(result['statistics']['subscriberCount']))
            video_counts.append(int(result['statistics']['videoCount']))
            view_counts.append(int(result['statistics']['viewCount']))

    channel_data['data'] = pd.DataFrame(zip(channel_ids, subscriber_counts, video_counts, view_counts),
                                        columns=['channel_id', 'subscriber_count', 'video_count', 'view_count'])
    channel_data['datetime'] = responses['datetime']

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'channel_stats', channel_data['datetime'])

    return channel_data


def get_channel_thumbnail_urls(responses, export_data=True):
    """Extracts channel thumbnail URLs for the specified Hololive Production member's channel from the API's responses.

    This functions extracts YouTube channel thumbnail (profile picture) URLs from YouTube Data API responses,
    exports a copy of the data (unless instructed otherwise), and returns the data along with its timestamp.

    Up to three URLs will be returned per channel for each resolution, i.e. default, medium, and high.
    URLs can then be used to download the images.

    Args:
        responses: Dictionary containing the YouTube Data API's responses to requests for YouTube channel data and a
                   timestamp, returned by youtube_api.request_data().
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        channel_data: Dictionary containing two key-value pairs:
                      1) 'data': Pandas DataFrame containing channel ids and their corresponding thumbnail URLs.
                      2) 'datetime': Datetime object specifying the date and time when the data was returned by the API.
    """
    channel_data = {}
    channel_ids = []
    default_thumbnail_urls = []
    medium_thumbnail_urls = []
    high_thumbnail_urls = []

    for response in responses['responses']:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            default_thumbnail_urls.append(result['snippet']['thumbnails']['default']['url'])
            medium_thumbnail_urls.append(result['snippet']['thumbnails']['medium']['url'])
            high_thumbnail_urls.append(result['snippet']['thumbnails']['high']['url'])

    channel_data['data'] = pd.DataFrame(zip(channel_ids, default_thumbnail_urls, medium_thumbnail_urls,
                                            high_thumbnail_urls),
                                        columns=['channel_id', 'default', 'medium', 'high'])
    channel_data['datetime'] = responses['datetime']

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'channel_thumbnail_urls', channel_data['datetime'])

    return channel_data


def get_uploads_playlist_ids(responses, export_data=True):
    """Extracts uploads playlist ids for the specified Hololive Production member's channel from the API's responses.

    This function extracts YouTube uploads playlist ids from YouTube Data API responses, exports a copy of the data
    (unless instructed otherwise), and returns the data along with its timestamp.

    Args:
        responses: Dictionary containing the YouTube Data API's responses to requests for YouTube channel data and a
                   timestamp, returned by youtube_api.request_data().
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        channel_data: Dictionary containing two key-value pairs:
                      1) 'data': Pandas DataFrame containing channel ids and their corresponding uploads playlist ids.
                      2) 'datetime': Datetime object specifying the date and time when the data was returned by the API.
    """
    channel_data = {}
    channel_ids = []
    uploads_playlist_ids = []

    for response in responses['responses']:
        results = response['items']
        for result in results:
            channel_ids.append(result['id'])
            uploads_playlist_ids.append(result['contentDetails']['relatedPlaylists']['uploads'])

    channel_data['data'] = pd.DataFrame(zip(channel_ids, uploads_playlist_ids),
                                        columns=['channel_id', 'uploads_playlist_id'])
    channel_data['datetime'] = responses['datetime']

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'uploads_playlist_ids', channel_data['datetime'])

    return channel_data
