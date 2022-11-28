from pathlib import Path
import pandas as pd
from holoanalytics import definitions as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats')


def request_session():
    """Displays available data collection sessions and returns the name of the session selected by the user.

    Returns:
        session_name: String specifying the data collection session to be opened.
    """

    session_dir_paths = list(df.YT_DAPI_SESSIONS_PATH.iterdir())
    max_index = len(session_dir_paths) - 1

    while True:

        print('Available YouTube Data API Sessions:')
        for index, file_path in enumerate(session_dir_paths):
            print(f'{index} - {file_path.name}')

        try:
            session_number = int(input('Please enter the session number for the data that will be prepared, e.g. 0: '))
        except ValueError:
            pass
        else:
            if 0 <= session_number <= max_index:
                break

        print('\nInvalid session number. Please try again.')

    session_name = session_dir_paths[session_number].name

    return session_name


def open_session(session):
    """Opens specified data collection session to allow access of collected data.

    Args:
        session: String specifying the data collection session to be opened.

    Returns:
        None
    """

    df.SESSION_PATH = df.YT_DAPI_SESSIONS_PATH / session


def import_video_data(member_names='all', data_types='all'):
    """Imports video data for specified Hololive Production members.

    Args:
        member_names: String or collection, e.g. list, of strings specifying the names of members whose video data is
                      to be imported. Default = 'all'.
        data_types: String or collection, e.g. list, of strings specifying the types of video data to be imported.
                    Default = 'all'.
    Returns:
        member_video_data: Dictionary of dictionaries, one for each member, of Pandas DataFrames containing video data.
    """

    member_names = _check_member_names(member_names)
    data_types = _check_data_types(data_types)

    member_video_data = _member_video_data(member_names, data_types)

    return member_video_data


def _check_member_names(member_names):

    if isinstance(member_names, str):
        if member_names.lower() == 'all':
            member_names = pd.read_csv(df.STARTING_DATA_FILE)['name']
        else:
            member_names = [member_names]
    elif isinstance(member_names, list) or isinstance(member_names, tuple) or isinstance(member_names, pd.Series):
        pass
    else:
        raise ValueError

    return member_names


def _check_data_types(data_types):

    if isinstance(data_types, str):
        if data_types.lower() == 'all':
            data_types = VIDEO_DATA_TYPES
        else:
            data_types = [data_types]
    elif isinstance(data_types, list) or isinstance(data_types, tuple) or isinstance(data_types, pd.Series):
        pass
    else:
        raise ValueError

    return data_types


def _member_video_data(member_names, data_types):
    member_video_data = {}

    for member_name in member_names:
        member_name = member_name.replace(' ', '_')
        member_dir_path = df.SESSION_PATH / 'Video' / member_name
        if member_dir_path.exists():
            member_video_data[member_name] = _all_video_data(member_dir_path, data_types)

    return member_video_data


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

