from pathlib import Path
import pandas as pd
from holoanalytics import definitions as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats')


def open_session(session):
    """Open specified data collection session to allow access of collected data.

    Args:
        session: String specifying the data collection session to be opened.

    Returns:
        None
    """

    df.SESSION_PATH = Path(df.PROJECT_ROOT / 'results' / 'collected' / 'YouTube' / 'YouTube-Data-API-Sessions'
                           / session)


def import_video_data(member_names='all', data_types='all'):
    """Import video data for specified Hololive Production members.

    Args:
        member_names: String or collection, e.g. list, of strings specifying the names of members whose video data is
                      to be imported. Default = 'all'.
        data_types: String or collection, e.g. list, of strings specifying the types of video data to be imported.
                    Default = 'all'.
    Returns:
        member_data: Dictionary of dictionaries, one for each member, of Pandas DataFrames containing video data.
    """

    if isinstance(member_names, str):
        if member_names.lower() == 'all':
            member_names = pd.read_csv(df.STARTING_DATA_FILE)['name']
        else:
            member_names = [member_names]
    elif isinstance(member_names, list) or isinstance(member_names, tuple) or isinstance(member_names, pd.Series):
        pass
    else:
        raise ValueError

    if isinstance(data_types, str):
        if data_types.lower() == 'all':
            data_types = VIDEO_DATA_TYPES
        else:
            data_types = [data_types]
    elif isinstance(data_types, list) or isinstance(data_types, tuple) or isinstance(data_types, pd.Series):
        pass
    else:
        raise ValueError

    members_video_data = _members_video_data(member_names, data_types)

    return members_video_data


def _members_video_data(member_names, data_types):
    members_video_data = {}

    for member_name in member_names:
        member_name = member_name.replace(' ', '_')
        member_dir_path = df.SESSION_PATH / 'Video' / member_name
        members_video_data[member_name] = _all_video_data(member_dir_path, data_types)

    return members_video_data


def _all_video_data(member_dir_path, data_types):
    video_data = {}

    for data_type in data_types:
        video_data[data_type] = _video_data(member_dir_path, data_type)

    return video_data


def _video_data(member_dir_path, data_type):
    data_file_path = None
    member_name = member_dir_path.name

    for file_path in member_dir_path.iterdir():
        if f'{member_name.lower()}_{data_type}' in file_path.name:
            data_file_path = file_path
            break

    if data_file_path is None:
        data = None
    else:
        data = pd.read_csv(data_file_path)

    return data

