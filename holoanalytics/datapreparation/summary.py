import calendar
from collections import Counter
from datetime import datetime
import pandas as pd
from pandas.core.dtypes.common import is_timedelta64_dtype
from holoanalytics.utils import exporting
from holoanalytics.datapreparation import reformatting as reform

VIDEO_STATS = ('view_count', 'like_count', 'comment_count')
VIDEO_TYPES = ('Normal', 'Short', 'Live Stream', 'Premiere')
CONTENT_TYPES = ('3DLive', 'Chatting', 'Collab', 'Debut', 'Drawing', 'Gaming', 'Karaoke', 'Music Video',
                 'Other', 'Outfit Reveal', 'Q&A', 'Review', 'Superchat Reading', 'VR', 'Watchalong')
START_YEAR = 2017  # Year when the first Hololive Production member debuted.
CURRENT_YEAR = datetime.now().year
GROUPS_BRANCHES_UNITS = {'Hololive': {'Japan': ('0th Generation', '1st Generation', '2nd Generation', '3rd Generation',
                                                '4th Generation', '5th Generation', '6th Generation', 'GAMERS'),
                                      'Indonesia': ('1st Generation', '2nd Generation', '3rd Generation'),
                                      'English': ('-Myth-', 'Project: HOPE', '-Council-')},
                         'Holostars': {'Japan': ('1st Generation', '2nd Generation', '3rd Generation', 'UPROAR!!'),
                                       'English': ('-TEMPUS-', )}}


def summarize_video_data(member_video_data, member_channel_data=None, export_data=True):
    """Summarizes YouTube video data on a per-channel basis.

    This function summarizes video attributes, video stats, video types, and content types.

    Args:
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing YouTube video data,
                           e.g. video stats, for individual Hololive Production members.
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        member_channel_data: Updated dictionary containing video summary data for each channel.
    """
    channel_data = {}
    member_summaries = []

    if member_channel_data is None:
        member_channel_data = {}

    for member_name, member_data in member_video_data.items():
        member_summary = {'member_data': {'name': member_name.replace('_', ' ')}}

        video_attributes = member_data['video_attributes']['data']
        video_stats = member_data['video_stats']['data']
        video_types = member_data['video_types']['data']
        content_types = member_data['content_types']['data']

        member_summary |= {'video_types': summarize_video_types(video_types)}
        member_summary |= {'video_attributes': summarize_video_attributes(video_attributes, video_types)}
        member_summary |= {'video_stats': summarize_video_stats(video_stats, video_types)}
        member_summary |= {'content_types': summarize_content_types(content_types, video_types)}

        member_summaries.append(member_summary)

    channel_data['data'] = pd.concat([pd.DataFrame.from_dict(member_summary).unstack()
                                      for member_summary in member_summaries], axis=1).dropna(how='all').transpose()
    channel_data['datetime'] = exporting.create_timestamp()

    member_channel_data['channel_video_summary'] = channel_data

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'channel_video_summary', channel_data['datetime'])

    return member_channel_data


def summarize_video_types(video_types):
    """Summarizes YouTube video type data by counting the number of videos for each type.

    This function summarizes based on the four video types: Normal, Short, Live Stream, and Premiere.

    Args:
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'live_stream_(count)', and the values are
                 the results, e.g. 100.
    """
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
    """Summarizes YouTube video attribute data.

    This function summarizes video durations and video publish datetimes.

    In all instances, summaries will be done on the videos as a whole, i.e. Video types do not matter.
    However, by passing in appropriate data to the 'video_types' parameter, additional summaries will also be done for
    individual video types, e.g. live streams.

    Args:
        video_attributes: Pandas DataFrame containing video ids and video attributes of YouTube videos.
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos. Default = None.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'live_stream_duration_(median)' and
                 the values are the results, e.g. 3:00:00.
    """
    summary = {}

    summary |= summarize_durations(video_attributes, video_types)
    summary |= summarize_publish_datetimes(video_attributes, video_types)

    return summary


def summarize_durations(video_attributes, video_types=None):
    """Summarizes YouTube video duration data.

    Args:
        video_attributes: Pandas DataFrame containing video ids and video attributes of YouTube videos.
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos. Default = None.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'video_duration_(max)' and the values are
                 the results, e.g. 8:08:51.
    """
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
    """Summarizes YouTube video publish datetime data.

    This function summarizes the video publish datetimes by counting the number of videos by month and year.

    In all instances, summaries will be done on the videos as a whole, i.e. Video types do not matter.
    However, by passing in appropriate data to the 'video_types' parameter, summaries will also be done for live streams
    specifically.

    Args:
        video_attributes: Pandas DataFrame containing video ids and video attributes of YouTube videos.
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos. Default = None.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'video_count_(2022)' and the values are
                 the results, e.g. 120.
    """
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
    """Summarizes YouTube video stat data.

    This function summarizes view, like, and comment counts.

    In all instances, summaries will be done on the videos as a whole, i.e. Video types do not matter.
    However, by passing in appropriate data to the 'video_types' parameter, additional summaries will also be done for
    individual video types, e.g. live streams.

    Args:
        video_stats: Pandas DataFrame containing video ids and video stats of YouTube videos.
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos. Default = None.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'view_count_(mean)' and the values are
                 the results, e.g. 750,000.
    """
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
    """Summarizes YouTube content type data.

    This function generates summary statistics for YouTube videos based on their content type.

    In all instances, summaries will be done on the videos as a whole, i.e. Video types do not matter.
    In a future update, there will be the option to summarize data based on video types.

    Args:
        content_types: Pandas DataFrame containing video ids and content types of YouTube videos.
        video_types: Pandas DataFrame containing video ids and video types of YouTube videos. Default = None.

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'music_video_(count)' and the values are
                 the results, e.g. 50.
    """
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
    """Generates summary statistics for a Pandas DataFrame column or Series.

    Summary statistics include: Count (Optional), Sum, Mean, Standard Deviation, 1st Quartile, Median, 3rd Quartile,
    Min, and Max.

    Args:
        data_col: Pandas DataFrame column / Pandas Series containing the data to be summarized.
        label: String specifying the type of data that is being summarized, e.g. 'view_count'.
        count: Boolean specifying if the number of data points / values are to be counted. Default = True.
        rounding: Integer representing the number of decimal places to round results to. Default = None (No rounding).

    Returns:
        summary: Dictionary where the keys specify the type of result, e.g. 'view_count_(median)' and the values are
                 the results, e.g. 200,000.
    """
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


def summarize_by_unit(member_data, member_channel_data, export_data=True):
    """Summarizes YouTube channel and video summary data for Hololive Production members on a per-unit basis.

    This function summarizes the specified data and, for each unit, returns the following results:
    - Member count
    - Subscriber count
    - Video count
    - View count
    - Total video duration
    - Total live stream duration
    
    Args:
        member_data: Pandas DataFrame containing starting data of Hololive Production members.
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.
        export_data: Boolean specifying whether the collected data is to be exported. Default = True.

    Returns:
        member_channel_data: Updated dictionary containing video summary data for each channel.
    """
    channel_data = {}
    unit_summaries = []

    member_data_multilevel = reform.convert_to_multilevel(member_data, 'member_data')
    channel_stats_multilevel = reform.convert_to_multilevel(member_channel_data['channel_stats']['data'],
                                                            'channel_stats')
    merged_data = member_data_multilevel.merge(channel_stats_multilevel,
                                               left_on=[('member_data', 'youtube_channel_id')],
                                               right_on=[('channel_stats', 'channel_id')])
    merged_data = merged_data.merge(member_channel_data['channel_video_summary']['data'],
                                    left_on=[('member_data', 'name')],
                                    right_on=[('member_data', 'name')])

    for group in GROUPS_BRANCHES_UNITS:
        group_data = merged_data.loc[merged_data[('member_data', 'group')] == group]

        for branch in GROUPS_BRANCHES_UNITS[group]:
            branch_data = group_data.loc[group_data[('member_data', 'branch')] == branch]

            for unit in GROUPS_BRANCHES_UNITS[group][branch]:
                unit_summary = {'group': group,  'branch': branch, 'unit': unit}

                unit_data = branch_data.loc[(branch_data[('member_data', 'unit')] == unit) |
                                            (branch_data[('member_data', 'unit2')] == unit)]

                # Note: More to be added later.
                unit_summary['member_count'] = len(unit_data.index)
                unit_summary['subscriber_count'] = unit_data[('channel_stats', 'subscriber_count')].sum()
                unit_summary['video_count'] = unit_data[('channel_stats', 'video_count')].sum()
                unit_summary['view_count'] = unit_data[('channel_stats', 'view_count')].sum()
                unit_summary['total_video_duration'] = unit_data[('video_attributes', 'video_duration_(sum)')].sum()
                unit_summary['total_live_stream_duration'] = unit_data[('video_attributes',
                                                                        'live_stream_duration_(sum)')].sum()

                unit_summaries.append(unit_summary)

    channel_data['data'] = pd.DataFrame(unit_summaries)
    channel_data['datetime'] = exporting.create_timestamp()

    member_channel_data['unit_summary'] = channel_data

    if export_data is True:
        exporting.export_channel_data(channel_data['data'], 'unit_summary', channel_data['datetime'])

    return member_channel_data
