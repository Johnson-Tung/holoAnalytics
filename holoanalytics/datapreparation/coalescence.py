from functools import reduce

import pandas as pd


def coalesce_video_data(starting_member_data, member_video_data):
    overall_video_data = {}
    merged_video_datasets = []

    member_names = starting_member_data['name']

    for member_name in member_names:
        member_name_key = member_name.replace(' ', '_')
        video_data = member_video_data.get(member_name_key, None)

        if video_data is None:
            continue

        video_attributes = video_data['video_attributes']['data']
        video_stats = video_data['video_stats']['data']
        video_types = video_data['video_types']['data']

        merged_video_data = reduce(lambda x, y: pd.merge(x, y, on='video_id'), [video_attributes, video_stats,
                                                                                video_types])

        merged_video_data.insert(0, 'member_name', member_name)

        merged_video_datasets.append(merged_video_data)

    overall_video_data['data'] = pd.concat(merged_video_datasets, ignore_index=True)

    member_video_data['Overall'] = overall_video_data

    return member_video_data

