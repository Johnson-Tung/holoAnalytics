from collections import Counter
from datetime import datetime
import pandas as pd
from holoanalytics.utils import exporting

VIDEO_DTYPES = ('video_attributes', 'video_stats', 'video_types', 'content_types')
VIDEO_STATS_DTYPES = ('view_count', 'like_count', 'comment_count')
VIDEO_TYPES_DTYPES = ('Normal', 'Short', 'Live Stream', 'Premiere')
START_YEAR = 2017  # Year when the first Hololive Production member debuted.
CURRENT_YEAR = datetime.now().year


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
    summary = {}

    counts = video_types.groupby('video_type').count()

    for video_type in counts.index:
        key = f'{video_type.lower().replace(" ", "_")}_(count)'
        summary[key] = counts.loc[video_type, counts.columns[0]]

    return summary


def summarize_video_attributes(video_attributes, video_types=None):
    pass


def summarize_video_stats(video_stats, video_types=None):
    pass


def summarize_content_types(content_types, video_types=None):
    summary = {}
    content_types_list = []

    for values in content_types['content_types']:
        values = eval(values) if isinstance(values, str) else values
        content_types_list += list(values)

    counts = Counter(content_types_list)

    for content_type, count in counts.items():
        summary[f'{content_type.lower().replace(" ", "_")}_(count)'] = count

    return summary

