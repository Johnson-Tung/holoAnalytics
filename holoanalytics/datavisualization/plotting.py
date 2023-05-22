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


def total_video_duration(member_data, member_channel_data, unit_time='hours', sort=None, colours=None):
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

    prepared_data, ordered_colours = _tvd_prepare(member_data, member_channel_data, unit_time, sort, colours)
    _tvd_plot(prepared_data, ordered_colours, unit_time)


def _tvd_prepare(member_data, member_channel_data, unit_time, sort, colours):

    member_name_col = ('member_data', 'name')
    video_duration_sum_col = ('video_attributes', 'video_duration_(sum)')

    member_data_multilevel = reformatting.convert_to_multilevel(member_data, 'member_data')
    merged_data = member_data_multilevel.merge(member_channel_data['channel_video_summary']['data'],
                                               on=[member_name_col])
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

    count_types = ('Subscriber', 'Video', 'View')

    group = group.title()
    branch = branch.title()

    if show.lower() == 'percentage':
        _channel_stats_by_unit_percentage(member_channel_data, group, branch, count_types)
    elif show.lower() == 'count':
        _channel_stats_by_unit_count(member_channel_data, group, branch, count_types)


def _channel_stats_by_unit_percentage(member_channel_data, group, branch, count_types):

    prepared_data = _csu_percentage_prepare(member_channel_data, group, branch, count_types)
    _csu_percentage_plot(prepared_data, group, branch, count_types)


def _csu_percentage_prepare(member_channel_data, group, branch, count_types):

    count_types_lower = [count_type.lower() for count_type in count_types]

    unit_summary_data = member_channel_data['unit_summary']['data']

    branch_data = unit_summary_data.loc[(unit_summary_data['group'] == group) &
                                        (unit_summary_data['branch'] == branch)].copy().reset_index()

    for count_type in count_types_lower:
        count_col = f'{count_type}_count'
        branch_data[f'{count_type}_percent'] = branch_data[count_col] / branch_data[count_col].sum() * 100

    return branch_data


def _csu_percentage_plot(data, group, branch, count_types):

    title_weight = 'bold'
    label_weight = 'bold'
    ticklabel_weight = 'bold'

    percentage_cols = [f'{count_type.lower()}_percent' for count_type in count_types]
    count_labels = [f'{count_type} Count' for count_type in count_types]

    fig, ax = plt.subplots()
    fig.set_size_inches(8.5, 11)

    previous_values = np.array([0, 0, 0])
    for index, row in data.iterrows():
        unit = row['unit']
        values = row[percentage_cols]
        ax.bar(x=count_labels, height=values, bottom=previous_values, label=unit.replace('_', ' '))
        previous_values = previous_values + np.array(values.copy())

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.suptitle(f'{group} {branch}:\nPercentages of {count_types[0]}s, {count_types[1]}s, and {count_types[2]}s '
                 f'by Unit', fontweight=title_weight)
    fig.tight_layout()

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1, 0.5), loc='center left', title='$\\bf{Units}$')

    # Removes "UserWarning: FixedFormatter should only be used together with FixedLocator"
    ax.set_xticks(ax.get_xticks())

    ax.set_xticklabels(ax.get_xticklabels(), weight=ticklabel_weight)

    for index, container in enumerate(ax.containers):
        labels = [f'{value:.2f}%' for value in data.loc[index, percentage_cols]]
        ax.bar_label(container, labels=labels)

    ax.set_ylabel('Percentage (%)', fontweight=label_weight)

    plt.show()


def _channel_stats_by_unit_count(member_channel_data, group, branch, count_types):

    prepared_data = _csu_count_prepare(member_channel_data, group, branch)
    _csu_count_plot(prepared_data, group, branch, count_types)


def _csu_count_prepare(member_channel_data, group, branch):

    unit_summary_data = member_channel_data['unit_summary']['data']

    branch_data = unit_summary_data.loc[(unit_summary_data['group'] == group) &
                                        (unit_summary_data['branch'] == branch)].copy().reset_index()

    return branch_data


def _csu_count_plot(data, group, branch, count_types):

    title_weight = 'bold'
    label_weight = 'bold'
    ticklabel_weight = 'bold'

    bars = []
    count_cols = [f'{count_type.lower()}_count' for count_type in count_types]

    fig, axes = plt.subplots(1, 3)
    fig.set_size_inches(8.5, 11)

    for ax, count_col in zip(axes, count_cols):
        bars.append(ax.bar(data['unit'], data[count_col]))

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.suptitle(f'{group} {branch}:\n{count_types[0]}, {count_types[1]}, and {count_types[2]} Counts by Unit',
                 fontweight=title_weight)
    fig.tight_layout()

    for ax, count_col in zip(axes, count_cols):
        ax.set_title(count_col.replace('_', ' ').title())
        ax.tick_params(axis='x', rotation=90)

        # Removes "UserWarning: FixedFormatter should only be used together with FixedLocator"
        ax.set_xticks(ax.get_xticks())

        ax.set_xticklabels(ax.get_xticklabels(), weight=ticklabel_weight)
        formatting.set_upper_limits(ax, max_y=data[count_col].max())

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


