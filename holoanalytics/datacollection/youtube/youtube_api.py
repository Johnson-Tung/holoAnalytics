"""Collection of YouTube data using the YouTube Data API

This module provides general tools for using the YouTube Data API to collect YouTube data.

Functions:
    1) initialize_api: Initializes a YouTube Data API client.
    2) request_data: Makes request(s) for YouTube data and receives responses.
"""

from datetime import datetime
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from holoanalytics.utils import data_reformatting as dr


def initialize_api():
    """Initializes a YouTube Data API client required to send requests for data and receive responses with data.

    Note: Initializing the client requires an API key. To get started, please follow the instructions on
    https://developers.google.com/youtube/v3/getting-started. The API key will need to be saved to your computer
    as an environment variable named "YouTube Data API v3 - API Key".

    Returns:
        client: YouTube Data API client.
    """

    api_key = os.environ.get('YouTube Data API v3 - API Key')
    client = build('youtube', 'v3', developerKey=api_key)

    return client


def request_data(client, resource_type, ids, max_results=50):
    """Makes request(s) for YouTube data using the YouTube Data API and returns the API's responses to the requests.

    Note: Please refer to https://developers.google.com/youtube/v3/docs for additional information about the API.

    Args:
        client: YouTube Data API client.
        resource_type: String specifying the type of resource that the API will interact with and retrieve data for,
                       e.g. A 'channel' resource represents a YouTube channel.
        ids: Sequence, e.g. Pandas Series, of strings representing ids for the resources involved,
             e.g. Channel ids of the channels for which data is being requested for.
        max_results: Integer representing the maximum number of results that the API can return in a single response.
                     Default = 50, the highest possible value.

    Returns:
        responses: List containing the API's responses to requests.
    """

    responses = {'responses': []}

    client_resource, part, resource_type = _check_resource_type(client, resource_type)

    if resource_type == 'playlistitems':
        token = None
        while True:
            request = client_resource.list(part='snippet', playlistId=ids,
                                           pageToken=token, maxResults=max_results)
            try:
                response = request.execute()
            except HttpError:  # If playlist is empty, YouTube API returns HttpError 404.
                break
            else:
                responses['responses'].append(response)
                token = response.get('nextPageToken', None)
                if token is None:
                    break
    else:
        batched_ids = dr.split_n_sized_batches(ids, max_results)
        for batch in batched_ids:
            request = client_resource.list(part=part, id=batch, maxResults=max_results)
            responses['responses'].append(request.execute())

    responses['datetime'] = datetime.utcnow().replace(microsecond=0)

    return responses


def _check_resource_type(client, resource_type):
    """Checks resource type, corrects if necessary, and prepares inputs for API request creation.

    Args:
        client: YouTube Data API client.
        resource_type: String specifying the type of resource that the API will interact with and retrieve data for,
                       e.g. A 'channel' resource represents a YouTube channel.

    Returns:
        client_resource: String representing the resource type for the YouTube API client.
        part: List of strings representing the input for the 'part' parameter when creating API requests,
              i.e. The type(s) of data to be requested for the specified resource.
        resource_type: String specifying the original or correct (if necessary) resource type.
    """

    if resource_type.lower() == 'channel' or resource_type.lower() == 'channels':
        client_resource = client.channels()
        part = ['contentDetails', 'snippet', 'statistics']
        resource_type = 'channels'
    elif resource_type.lower() == 'playlistitem' or resource_type.lower() == 'playlistitems':
        client_resource = client.playlistItems()
        part = ['snippet']
        resource_type = 'playlistitems'
    elif resource_type.lower() == 'video' or resource_type.lower() == 'videos':
        client_resource = client.videos()
        part = ['snippet', 'contentDetails', 'statistics', 'liveStreamingDetails']
        resource_type = 'videos'
    else:
        raise NameError('Resource type invalid or not supported.')

    return client_resource, part, resource_type


