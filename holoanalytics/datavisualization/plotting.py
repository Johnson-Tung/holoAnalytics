import string

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from holoanalytics.datapreparation import reformatting
from holoanalytics.datavisualization import formatting


def channel_stats_correlation(member_data, member_channel_data, colours=None):
    """Plots the correlations between different types of channel stat data for Hololive Production members.

    This function will create three plots:
    - View Count vs. Subscriber Count
    - Subscriber Count vs. Video Count
    - View Count vs. Video Count

    Args:
        member_data: Pandas DataFrame containing starting data of Hololive Production members.
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.

    Returns:
        None
    """

    prepared_data, ordered_colours = _csc_prepare(member_data, member_channel_data, colours)
    _csc_plot(prepared_data, ordered_colours)


def _csc_prepare(member_data, member_channel_data, colours):

    plot_data = member_data.merge(member_channel_data['channel_stats']['data'],
                                  left_on='youtube_channel_id', right_on='channel_id')
    prepared_data, ordered_colours = merge_colour_data(plot_data, colours)

    return prepared_data, ordered_colours


def _csc_plot(data, ordered_colours):
    title_size = 11
    title_weight = 'bold'
    label_weight = 'bold'
    scale = 1_000_000

    fig, axes = plt.subplots(1, 3)
    fig.set_size_inches(11, 8.5)

    channel_stat_types = ('Subscriber Count', 'View Count', 'Video Count')
    correlations = ((channel_stat_types[0], channel_stat_types[1]),
                    (channel_stat_types[2], channel_stat_types[0]),
                    (channel_stat_types[2], channel_stat_types[1]))

    for index, (ax, correlation) in enumerate(zip(axes, correlations)):

        x, y = correlation
        x_col, y_col = [channel_stat.replace(' ', '_').lower() for channel_stat in correlation]
        x_unit, y_unit = [' (millions)' if channel_stat in channel_stat_types[0:-1] else ''
                          for channel_stat in correlation]

        ax.scatter(x=data[x_col] / scale, y=data[y_col] / scale, color=ordered_colours)

        ax.set_xlabel(f'{x}{x_unit}', fontweight=label_weight)
        ax.set_ylabel(f'{y}{y_unit}', fontweight=label_weight)
        ax.set_title(f'{y} vs. {x}', fontsize=title_size, fontweight=title_weight)

        formatting.set_upper_limits(ax, max_x=data[x_col].max() / scale, max_y=data[y_col].max() / scale)
        ax.get_xaxis().get_major_formatter().set_scientific(False)
        ax.get_yaxis().get_major_formatter().set_scientific(False)
        ax.tick_params(axis='x', rotation=90)
        ax.set_xlim(0)
        ax.set_ylim(0)

        if index == 2:
            ax.yaxis.set_label_position('right')
            ax.yaxis.tick_right()

    fig.suptitle('Correlation of Subscriber, Video, and View Counts of Hololive Production Members',
                 fontsize=13, fontweight=title_weight)
    fig.tight_layout()

    plt.show()


def total_video_duration(member_data, channel_video_summary, unit_time='hours', sort=None, colours=None):
    """Plots the total video duration in days, hours, minutes, or seconds for individual Hololive Production members.

    Args:
        member_data: Pandas DataFrame containing starting data of Hololive Production members.
        channel_video_summary: Pandas DataFrame containing video summary data.
        unit_time: String specifying the unit of time that the total video duration is to be presented in,
                   i.e. 'days', 'hours', 'minutes', or 'seconds'. Default = 'hours'.
        sort: String specifying if the data is to be sorted, i.e. 'ascending' or 'descending'.
              Default = None (No sorting).

    Returns:
        None
    """

    prepared_data, ordered_colours = _tvd_prepare(member_data, channel_video_summary, unit_time, sort, colours)
    _tvd_plot(prepared_data, ordered_colours, unit_time)


def _tvd_prepare(member_data, channel_video_summary, unit_time, sort, colours):

    member_name_col = ('member_data', 'name')
    video_duration_sum_col = ('video_attributes', 'video_duration_(sum)')

    member_data_multilevel = reformatting.convert_to_multilevel(member_data, 'member_data')
    merged_data = member_data_multilevel.merge(channel_video_summary['data'], on=[member_name_col])
    plot_data = merged_data[[member_name_col, video_duration_sum_col]].copy()
    plot_data[video_duration_sum_col] = plot_data[video_duration_sum_col].apply(reformatting.convert_timedelta,
                                                                                unit=unit_time)

    if sort is None:
        pass
    elif not isinstance(sort, str):
        raise TypeError("The argument for the 'sort' parameter needs to be None or a string with a value of "
                        "'ascending' or 'descending'.")
    elif sort.lower() == 'ascending':
        plot_data.sort_values(('video_attributes', 'video_duration_(sum)'), ascending=True, inplace=True)
    elif sort.lower() == 'descending':
        plot_data.sort_values(('video_attributes', 'video_duration_(sum)'), ascending=False, inplace=True)
    else:
        raise ValueError("The argument for the 'sort' parameter is a string but needs to be 'ascending' "
                         "or 'descending'.")

    prepared_data, ordered_colours = merge_colour_data(plot_data, colours)

    return prepared_data, ordered_colours


def _tvd_plot(data, ordered_colours, unit_time):

    title_weight = 'bold'
    label_weight = 'bold'
    unit_time_label = string.capwords(unit_time)

    fig, ax = plt.subplots()
    ax.bar(x=data[('member_data', 'name')], height=data[('video_attributes', 'video_duration_(sum)')],
           color=ordered_colours)

    fig.set_size_inches(8.5, 11)
    fig.suptitle(f'Number of {unit_time_label} of Video Content of Hololive Production Members',
                 fontweight=title_weight)
    fig.tight_layout()

    ax.tick_params(axis='x', rotation=90)
    ax.set_xlabel('Members', fontweight=label_weight)
    ax.set_ylabel(f'Number of {unit_time_label}', fontweight=label_weight)
    formatting.set_upper_limits(ax, max_y=data[('video_attributes', 'video_duration_(sum)')].max())

    plt.show()


def channel_stats_by_unit(member_channel_data, group, branch, show):
    """Plots channel stat data for Hololive Production members on a per-unit basis for a specific group and branch.

    Args:
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.
        group: String specifying the group of interest.
        branch: String specifying the branch of interest.
        show: String specifying if the percentages or counts are to be shown, i.e. 'percentage' or 'count'.

    Returns:
        None
    """

    if show.lower() == 'percentage':
        _channel_stats_by_unit_percentage(member_channel_data, group, branch)
    elif show.lower() == 'count':
        _channel_stats_by_unit_count(member_channel_data, group, branch)


def _channel_stats_by_unit_percentage(member_channel_data, group, branch):
    title_weight = 'bold'
    label_weight = 'bold'
    ticklabel_weight = 'bold'

    data_cols = ['subscriber_percent', 'video_percent', 'view_percent']

    group = group.title()
    branch = branch.title()

    # Prepare Data
    unit_summary_data = member_channel_data['unit_summary']['data']

    branch_data = unit_summary_data.loc[(unit_summary_data['group'] == group) &
                                        (unit_summary_data['branch'] == branch)].copy().reset_index()

    branch_data['subscriber_percent'] = branch_data['subscriber_count'] / branch_data['subscriber_count'].sum() * 100
    branch_data['video_percent'] = branch_data['video_count'] / branch_data['video_count'].sum() * 100
    branch_data['view_percent'] = branch_data['view_count'] / branch_data['view_count'].sum() * 100

    # Create Plot
    fig, ax = plt.subplots()
    fig.set_size_inches(8.5, 11)

    previous_values = np.array([0, 0, 0])
    for index, row in branch_data.iterrows():
        unit = row['unit']
        values = row[data_cols]
        ax.bar(x=['Subscriber Count', 'Video Count', 'View Count'], height=values, bottom=previous_values,
               label=unit.replace('_', ' '))
        previous_values = previous_values + np.array(values.copy())

    # Format Plot
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.suptitle(f'{group} {branch}:\nPercentages of Subscribers, Videos, and Views by Unit', fontweight=title_weight)
    fig.tight_layout()

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1, 0.5), loc='center left', title='$\\bf{Units}$')

    # Removes "UserWarning: FixedFormatter should only be used together with FixedLocator"
    ax.set_xticks(ax.get_xticks())

    ax.set_xticklabels(ax.get_xticklabels(), weight=ticklabel_weight)

    for index, container in enumerate(ax.containers):
        labels = [f'{value:.2f}%' for value in branch_data.loc[index, data_cols]]
        ax.bar_label(container, labels=labels)

    ax.set_ylabel('Percentage (%)', fontweight=label_weight)

    plt.show()


def _channel_stats_by_unit_count(member_channel_data, group, branch):
    title_weight = 'bold'
    label_weight = 'bold'
    ticklabel_weight = 'bold'

    group = group.title()
    branch = branch.title()

    # Prepare Data
    unit_summary_data = member_channel_data['unit_summary']['data']

    branch_data = unit_summary_data.loc[(unit_summary_data['group'] == group) &
                                        (unit_summary_data['branch'] == branch)].copy().reset_index()

    # Create Plot
    bars = []
    data_cols = ['subscriber_count', 'video_count', 'view_count']

    fig, axes = plt.subplots(1, 3)
    fig.set_size_inches(8.5, 11)

    for ax, data_col in zip(axes, data_cols):
        bars.append(ax.bar(branch_data['unit'], branch_data[data_col]))

    # Format Plot
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.suptitle(f'{group} {branch}:\nSubscriber, Video, and View Counts by Unit', fontweight=title_weight)
    fig.tight_layout()

    for ax, data_col in zip(axes, data_cols):
        ax.set_title(data_col.replace('_', ' ').title())
        ax.tick_params(axis='x', rotation=90)

        # Removes "UserWarning: FixedFormatter should only be used together with FixedLocator"
        ax.set_xticks(ax.get_xticks())

        ax.set_xticklabels(ax.get_xticklabels(), weight=ticklabel_weight)
        formatting.set_upper_limits(ax, max_y=branch_data[data_col].max())

    for index, ax in enumerate(axes):
        ax.bar_label(bars[index], rotation=90, fontweight=label_weight)

    plt.show()


def merge_colour_data(plot_data, colour_data):

    if not isinstance(plot_data, pd.DataFrame):
        raise TypeError("The argument for the 'youtube_data' parameter needs to be a Pandas DataFrame, not a(n) "
                        f"{type(plot_data)}.")

    if colour_data is None:
        merged_data = plot_data
        ordered_colours = None
    elif isinstance(colour_data, pd.DataFrame):
        column_levels = plot_data.columns.nlevels

        if column_levels == 1:
            merged_data = plot_data.merge(colour_data, on='name')
            ordered_colours = merged_data['colour']
        elif column_levels == 2:
            new_level_name = 'member_plot_colours'
            colour_data = reformatting.convert_to_multilevel(colour_data, new_level_name)
            merged_data = plot_data.merge(colour_data, left_on=[('member_data', 'name')],
                                          right_on=[(new_level_name, 'name')])
            ordered_colours = merged_data[(new_level_name, 'colour')]
        else:
            raise ValueError  # TODO: Raise a more appropriate error and include an error message.
    else:
        raise TypeError("The argument for the 'colour_data' parameter needs to be a Pandas DataFrame, not a(n) "
                        f"{type(colour_data)}.")

    return merged_data, ordered_colours


