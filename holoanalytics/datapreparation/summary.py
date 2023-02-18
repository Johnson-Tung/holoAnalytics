from collections import Counter
from datetime import datetime
import pandas as pd
from holoanalytics.utils import exporting

VIDEO_DTYPES = ('video_attributes', 'video_stats', 'video_types', 'content_types')
VIDEO_STATS_DTYPES = ('view_count', 'like_count', 'comment_count')
VIDEO_TYPES_DTYPES = ('Normal', 'Short', 'Live Stream', 'Premiere')
CONTENT_TYPES = ('3DLive', 'Chatting', 'Collab', 'Debut', 'Drawing', 'Gaming', 'Karaoke', 'Music Video',
                 'Other', 'Outfit Reveal', 'Q&A', 'Review', 'Superchat Reading', 'VR', 'Watchalong')
START_YEAR = 2017  # Year when the first Hololive Production member debuted.
CURRENT_YEAR = datetime.now().year


def summarize_video_data(member_channel_data, member_video_data, export_data=True):
    member_summaries = []
    member_names = []

    if 'Summaries' not in member_channel_data.keys():
        member_channel_data['Summaries'] = {}

    for member_name, member_data in member_video_data.items():
        member_summary = {}

        video_attributes = member_data['video_attributes']
        video_stats = member_data['video_stats']
        video_types = member_data['video_types']
        content_types = member_data['content_types']

        member_summary |= {'video_types': summarize_video_types(video_types)}
        member_summary |= {'video_attributes': summarize_video_attributes(video_attributes, video_types)}
        member_summary |= {'video_stats': summarize_video_stats(video_stats, video_types)}
        member_summary |= {'content_types': summarize_content_types(content_types, video_types)}

        member_summaries.append(member_summary)
        member_names.append(member_name)

    data = pd.concat([pd.DataFrame.from_dict(member_summary).unstack()
                      for member_summary in member_summaries], axis=1).dropna(how='all').transpose()
    data.insert(0, 'member_name', member_names)

    member_channel_data['Summaries']['video_data'] = data

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
    summary = {}

    for video_stat in VIDEO_STATS_DTYPES:
        summary |= summary_stats(video_stats[video_stat], video_stat, count=False)

    if video_types is not None:
        merged_data = video_stats.merge(video_types, on='video_id')

        for video_type in VIDEO_TYPES_DTYPES:
            filtered_data = merged_data.loc[merged_data['video_type'] == video_type, VIDEO_STATS_DTYPES]

            for video_stat in VIDEO_STATS_DTYPES:
                summary |= summary_stats(filtered_data[video_stat],
                                         f'{video_type.lower().replace(" ", "_")}_{video_stat}', count=False)

    return summary


def summarize_content_types(content_types, video_types=None):
    summary = {}
    all_content_types = []

    for ct_set in content_types['content_types']:
        ct_set = eval(ct_set) if isinstance(ct_set, str) else ct_set
        all_content_types += list(ct_set)

    counts = Counter(all_content_types)

    for content_type in CONTENT_TYPES:
        label = f'{content_type.lower().replace(" ", "_")}_(count)'
        if content_type in counts.keys():
            summary[label] = counts[content_type]
        else:
            summary[label] = 0

    return summary


def summary_stats(data_col, label, count=True):
    pass

