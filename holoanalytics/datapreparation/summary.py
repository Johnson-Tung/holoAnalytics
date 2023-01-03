import pandas as pd
from holoanalytics.utils import exporting


def summarize_video_data(member_channel_data, member_video_data, export_data=True):
    member_summaries = []

    for member_name, member_data in member_video_data.items():
        summary = {'name': member_name}

        video_attributes = member_data['video_attributes']
        video_stats = member_data['video_stats']
        video_types = member_data['video_types']
        content_types = member_data['content_types']

        summary |= summarize_video_types(video_types)
        summary |= summarize_video_attributes(video_attributes, video_types)
        summary |= summarize_video_stats(video_stats, video_types)
        summary |= summarize_content_types(content_types, video_types)

        member_summaries.append(summary)

    data = pd.DataFrame(member_summaries)

    member_channel_data['channel_video_summaries'] = data

    exporting.export_channel_data(data, export_data, 'channel_video_summaries')

    return member_channel_data


def summarize_video_types(video_types):
    pass


def summarize_video_attributes(video_attributes, video_types=None):
    pass


def summarize_video_stats(video_stats, video_types=None):
    pass


def summarize_content_types(content_types, video_types=None):
    pass

