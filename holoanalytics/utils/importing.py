import csv
from datetime import datetime
import pandas as pd
from holoanalytics.settings import session
from holoanalytics.settings import core as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats',  # Collected
                    'content_types', 'video_title_keywords', 'video_types')  # Prepared
CHANNEL_DATA_TYPES = ('channel_stats', 'channel_thumbnail_urls', 'channel_titles', 'uploads_playlist_ids',  # Collected
                      'channel_video_summary', 'unit_summary')  # Prepared


def request_session(session_open=True):
    """Displays available sessions and allows the user to request a specific session.

    Args:
        session_open: Boolean specifying if the requested session is to be opened. Default = True.

    Returns:
        session_name: String specifying the name of the requested session.
    """

    session_dir_paths = list(df.YT_DAPI_SESSIONS_PATH.iterdir())
    max_index = len(session_dir_paths) - 1

    while True:

        print('Available YouTube Data API Sessions:')
        for index, file_path in enumerate(session_dir_paths):
            print(f'{index} - {file_path.name}')

        try:
            session_number = int(input('Please select a session and enter the number beside its name, '
                                       'e.g. Enter "0" if you wish to select the first session: '))
        except ValueError:
            pass
        else:
            if 0 <= session_number <= max_index:
                break

        print('\nInvalid session selected. Please try again and enter a valid number.')

    session_name = session_dir_paths[session_number].name

    if session_open is True:
        open_session(session_name)

    return session_name


def open_session(session_name):
    """Opens the specified session and allows access to its data, i.e. collected data and any prepared data.

    Args:
        session: String specifying the data collection session to be opened.

    Returns:
        None
    """

    session.SESSION_PATH = df.YT_DAPI_SESSIONS_PATH / session_name


def import_member_data():
    """Imports starting data for Hololive Production members.

    Starting data includes, but is not limited to, details such as names, groups, unit names, and debut dates.

    Returns:
        member_data: Pandas DataFrame containing starting data of Hololive Production members.
    """

    member_data = pd.read_csv(df.STARTING_DATA_FILE)

    return member_data


def import_channel_data(channel_data_types='all'):
    """Imports YouTube channel data for specified Hololive Production members.

    Args:
        channel_data_types: String or collection, e.g. list, of strings specifying the types of channel data to be
                            imported. Default = 'all'.

    Returns:
        member_channel_data: Dictionary of Pandas DataFrames containing YouTube channel data, e.g. channel stats,
                             for Hololive Production members.
    """

    channel_data_types = _check_data_subtypes('channel', channel_data_types)
    member_channel_data = _get_member_channel_data(channel_data_types)

    return member_channel_data


def _get_member_channel_data(channel_data_types):
    member_channel_data = {}

    dir_path = session.SESSION_PATH / 'Channel'

    if not dir_path.exists():
        raise FileNotFoundError("This session does not have a 'Channel' data folder.")

    file_paths = list(dir_path.iterdir())
    file_paths.reverse()  # Get file paths from newest to oldest

    for channel_data_type in channel_data_types:
        member_channel_data[channel_data_type] = _get_channel_data(file_paths, channel_data_type)

    return member_channel_data


def _get_channel_data(file_paths, channel_data_type):
    channel_data = {}

    for file_path in file_paths:
        if f'{channel_data_type}' in file_path.name:
            headers = [0, 1] if channel_data_type == 'channel_video_summary' else [0]  # Note: Temporary solution

            channel_data['data'] = pd.read_csv(file_path, header=headers)
            channel_data['datetime'] = _extract_datetime(file_path.stem)
            break

    return channel_data


def _extract_datetime(file_name):

    year = int(file_name[0:4])
    month = int(file_name[5:7])
    day = int(file_name[8:10])
    hour = int(file_name[11:13])
    minute = int(file_name[13:15])

    file_datetime = datetime(year, month, day, hour, minute)

    return file_datetime


def import_video_data(member_names='all', video_data_types='all'):
    """Imports YouTube video data for specified Hololive Production members.

    Args:
        member_names: String or collection, e.g. list, of strings specifying the names of members whose video data is
                      to be imported. Default = 'all'.
        video_data_types: String or collection, e.g. list, of strings specifying the types of video data to be imported.
                          Default = 'all'.
    Returns:
        member_video_data: Dictionary of dictionaries, one for each member, of Pandas DataFrames containing video data.
    """

    member_names = _check_member_names(member_names)
    video_data_types = _check_data_subtypes('video', video_data_types)

    member_video_data = _get_member_video_data(member_names, video_data_types)

    return member_video_data


def _check_member_names(member_names):

    if isinstance(member_names, str):
        if member_names.lower() == 'all':
            member_names = pd.read_csv(df.STARTING_DATA_FILE)['name']
        else:
            member_names = [member_names]
    elif isinstance(member_names, (list, tuple, pd.Series)):
        pass
    else:
        raise ValueError

    return member_names


def _check_data_subtypes(data_type, data_subtypes):

    if isinstance(data_subtypes, str):
        if data_subtypes.lower() == 'all':
            if data_type.lower() == 'channel':
                data_subtypes = CHANNEL_DATA_TYPES
            elif data_type.lower() == 'video':
                data_subtypes = VIDEO_DATA_TYPES
            else:
                raise ValueError("Invalid value for 'data_type' was given. Valid values: 'channel' and 'video'.")
        else:
            data_subtypes = [data_subtypes]
    elif isinstance(data_subtypes, (list, tuple, pd.Series)):
        pass
    else:
        raise ValueError

    return data_subtypes


def _get_member_video_data(member_names, video_data_types):
    member_video_data = {}

    for member_name in member_names:
        member_name = member_name.replace(' ', '_')
        member_dir_path = session.SESSION_PATH / 'Video' / member_name
        if member_dir_path.exists():
            member_video_data[member_name] = _get_single_member_video_data(member_dir_path, video_data_types)

    return member_video_data


def _get_single_member_video_data(member_dir_path, video_data_types):
    single_member_video_data = {}

    member_name = member_dir_path.name
    file_paths = list(member_dir_path.iterdir())
    file_paths.reverse()  # Get file paths from newest to oldest

    for video_data_type in video_data_types:
        single_member_video_data[video_data_type] = _get_video_data(member_name, file_paths, video_data_type)

    return single_member_video_data


def _get_video_data(member_name, file_paths, video_data_type):
    video_data = {}

    for file_path in file_paths:
        if f'{member_name.lower()}_{video_data_type}' in file_path.name:
            video_data['data'] = pd.read_csv(file_path)
            video_data['datetime'] = _extract_datetime(file_path.stem)
            break

    return video_data


def import_keyword_banks(*languages):
    """Imports keyword banks for the specified language(s).

    Note: Languages must be spelled out in full, e.g. 'english'.

    Args:
        *languages: One or more strings specifying the languages for the keyword banks to be imported.

    Returns:
        keyword_banks: Dictionary of dictionaries of sets containing keyword group names and keywords for the languages
                       specified, i.e. keyword_banks[language][keyword_group] = {keywords}.
    """
    keyword_banks = {}

    languages = [language.lower() for language in languages]

    for language in languages:
        if language in ['english', 'japanese', 'indonesian']:
            keyword_banks[language.title()] = _import_keyword_bank(language)

    return keyword_banks


def _import_keyword_bank(language):
    keyword_bank = {}
    file_path = df.KEYWORD_BANKS_PATH / f'{language}_video_title_keywords.csv'

    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        data = csv.reader(file)
        for row in data:
            keyword_bank[row[0]] = set(row[1:]) if len(row) > 1 else set()

    return keyword_bank


def get_groups_branches_units(starting_data):
    """Gets all group, branch, and unit names for Hololive Production members from the starting data.

    Args:
        starting_data: Pandas DataFrame containing starting data of Hololive Production members.

    Returns:
        groups_branches_units: Dictionary of dictionaries of tuples of strings specifying the names of every unit
                               in Hololive Production organized based on their branch and group,
                               i.e. groups_branches_units[group_name][branch_name] = (unit_name1, unit_name2, ...).
    """
    groups_branches_units = {}

    for group in starting_data['group'].unique():
        groups_branches_units[group] = {}
        group_data = starting_data.loc[starting_data['group'] == group]

        for branch in group_data['branch'].unique():
            groups_branches_units[group][branch] = []
            branch_data = group_data.loc[group_data['branch'] == branch]

            for unit in branch_data['unit'].unique():
                groups_branches_units[group][branch].append(unit)

            groups_branches_units[group][branch] = tuple(groups_branches_units[group][branch])

    return groups_branches_units


def import_member_plot_colours():

    member_colours = pd.read_csv(df.MEMBER_PLOT_COLOURS_PATH)

    return member_colours
