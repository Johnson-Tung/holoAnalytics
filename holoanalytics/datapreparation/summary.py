import calendar
from collections import Counter
from datetime import datetime
import pandas as pd
from pandas.core.dtypes.common import is_timedelta64_dtype
from holoanalytics.utils import exporting

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats', 'video_types', 'content_types')
VIDEO_STATS = ('view_count', 'like_count', 'comment_count')
VIDEO_TYPES = ('Normal', 'Short', 'Live Stream', 'Premiere')
CONTENT_TYPES = ('3DLive', 'Chatting', 'Collab', 'Debut', 'Drawing', 'Gaming', 'Karaoke', 'Music Video',
                 'Other', 'Outfit Reveal', 'Q&A', 'Review', 'Superchat Reading', 'VR', 'Watchalong')
START_YEAR = 2017  # Year when the first Hololive Production member debuted.
CURRENT_YEAR = datetime.now().year


def summarize_video_data(member_video_data, member_channel_data=None, export_data=True):
    member_summaries = []

    if member_channel_data is None:
        member_channel_data = {}

    for member_name, member_data in member_video_data.items():
        member_summary = {'member_data': {'member_name': member_name.replace('_', ' ')}}

        video_attributes = member_data['video_attributes']
        video_stats = member_data['video_stats']
        video_types = member_data['video_types']
        content_types = member_data['content_types']

        member_summary |= {'video_types': summarize_video_types(video_types)}
        member_summary |= {'video_attributes': summarize_video_attributes(video_attributes, video_types)}
        member_summary |= {'video_stats': summarize_video_stats(video_stats, video_types)}
        member_summary |= {'content_types': summarize_content_types(content_types, video_types)}

        member_summaries.append(member_summary)

    data = pd.concat([pd.DataFrame.from_dict(member_summary).unstack()
                      for member_summary in member_summaries], axis=1).dropna(how='all').transpose()

    member_channel_data['channel_video_summary'] = data

    exporting.export_channel_data(data, export_data, 'channel_video_summary')

    return member_channel_data


def summarize_video_types(video_types):
    summary = {}

    counts = video_types.groupby('video_type').count()

    for video_type in VIDEO_TYPES:
        key = f'{video_type.lower().replace(" ", "_")}_(count)'
        if video_type in counts.index:
            summary[key] = counts.loc[video_type, counts.columns[0]]
        else:
            summary[key] = 0

    return summary


def summarize_video_attributes(video_attributes, video_types=None):
    summary = {}

    summary |= summarize_durations(video_attributes, video_types)
    summary |= summarize_publish_datetimes(video_attributes, video_types)

    return summary


def summarize_durations(video_attributes, video_types=None):
    summary = {}

    durations = video_attributes['duration']

    summary |= summary_stats(durations, 'video_duration', count=False)

    if isinstance(video_types, pd.DataFrame):
        merged_data = video_attributes[['video_id', 'duration']].merge(video_types, on='video_id')

        for video_type in VIDEO_TYPES:
            filtered_data = merged_data.loc[merged_data['video_type'] == video_type, 'duration']
            summary |= summary_stats(filtered_data, f'{video_type.lower().replace(" ", "_")}_duration',
                                     count=False)

    return summary


def summarize_publish_datetimes(video_attributes, video_types=None):
    summary = {}

    publish_datetimes = video_attributes['publish_datetime']

    summary |= _count_by_year(publish_datetimes, 'video')
    summary |= _count_by_month(publish_datetimes, 'video')

    if isinstance(video_types, pd.DataFrame):
        merged_data = video_attributes[['video_id', 'publish_datetime']].merge(video_types, on='video_id')
        publish_datetimes_ls = merged_data.loc[merged_data['video_type'] == 'Live Stream', 'publish_datetime']

        summary |= _count_by_year(publish_datetimes_ls, 'live_stream')
        summary |= _count_by_month(publish_datetimes_ls, 'live_stream')

    return summary


def _count_by_year(publish_datetimes, video_type):
    summary = {}

    counts_year = publish_datetimes.groupby(publish_datetimes.dt.year).count()

    for year in range(START_YEAR, CURRENT_YEAR+1):
        label = f'{video_type}_count_({year})'
        if year in counts_year.index:
            summary[label] = counts_year[year]
        else:
            summary[label] = 0

    return summary


def _count_by_month(publish_datetimes, video_type):
    summary = {}

    counts_month = publish_datetimes.groupby(publish_datetimes.dt.month).count()

    for month_number in range(1, 13):
        month = calendar.month_name[month_number].lower()
        label = f'{video_type}_count_({month})'
        if month_number in counts_month.index:
            summary[label] = counts_month[month_number]
        else:
            summary[label] = 0

    return summary


def summarize_video_stats(video_stats, video_types=None):
    summary = {}

    for video_stat in VIDEO_STATS:
        summary |= summary_stats(video_stats[video_stat], video_stat, count=False)

    if video_types is not None:
        merged_data = video_stats.merge(video_types, on='video_id')

        for video_type in VIDEO_TYPES:
            filtered_data = merged_data.loc[merged_data['video_type'] == video_type, VIDEO_STATS]

            for video_stat in VIDEO_STATS:
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


def summary_stats(data_col, label, count=True, rounding=None):
    summary = {}

    # Counting the number of data points is sometimes unnecessary and redundant. Therefore, make it optional.
    if count is True:
        summary[f'{label}_(count)'] = data_col.count()

    summary[f'{label}_(sum)'] = data_col.sum()
    summary[f'{label}_(mean)'] = data_col.mean()

    # std() returns NaN if there is only one data point. There is no deviation, so replace NaN with zero.
    std = data_col.std()
    summary[f'{label}_(std)'] = 0 if pd.isna(std) else std

    summary[f'{label}_(q1)'] = data_col.quantile(0.25)
    summary[f'{label}_(median)'] = data_col.median()
    summary[f'{label}_(q3)'] = data_col.quantile(0.75)

    summary[f'{label}_(min)'] = data_col.min()
    summary[f'{label}_(max)'] = data_col.max()

    if not is_timedelta64_dtype(data_col) and isinstance(rounding, int):
        for key, value in summary.items():
            summary[key] = round(value, rounding)

    return summary
