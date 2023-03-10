import csv
from datetime import datetime
import pandas as pd
from holoanalytics import definitions as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats',  # Collected
                    'content_types', 'video_title_keywords', 'video_types')  # Prepared
CHANNEL_DATA_TYPES = ('channel_stats', 'channel_thumbnail_urls', 'channel_titles', 'uploads_playlist_ids',  # Collected
                      'channel_video_summary')  # Prepared


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


def open_session(session):
    """Opens the specified session and allows access to its data, i.e. collected data and any prepared data.

    Args:
        session: String specifying the data collection session to be opened.

    Returns:
        None
    """

    df.SESSION_PATH = df.YT_DAPI_SESSIONS_PATH / session


def import_member_data():

    member_data = pd.read_csv(df.STARTING_DATA_FILE)

    return member_data


def import_channel_data(channel_data_types='all'):

    channel_data_types = _check_data_subtypes('channel', channel_data_types)
    member_channel_data = _get_member_channel_data(channel_data_types)

    return member_channel_data


def _get_member_channel_data(channel_data_types):
    member_channel_data = {}

    dir_path = df.SESSION_PATH / 'Channel'

    if not dir_path.exists():
        raise FileNotFoundError("This session does not have a 'Channel' data folder.")

    file_paths = list(dir_path.iterdir())
    file_paths.reverse()  # Get file paths from latest to earliest.

    for channel_data_type in channel_data_types:
        for file_path in file_paths:
            if f'{channel_data_type}' in file_path.name:
                member_channel_data[channel_data_type] = {}

                headers = [0, 1] if channel_data_type == 'channel_video_summary' else [0]  # Note: Temporary solution.

                member_channel_data[channel_data_type]['data'] = pd.read_csv(file_path, header=headers)
                member_channel_data[channel_data_type]['datetime'] = _extract_datetime(file_path.stem)

                break

    return member_channel_data


def _extract_datetime(file_name):

    year = int(file_name[0:4])
    month = int(file_name[5:7])
    day = int(file_name[8:10])
    hour = int(file_name[11:13])
    minute = int(file_name[13:15])

    file_datetime = datetime(year, month, day, hour, minute)

    return file_datetime


def import_video_data(member_names='all', video_data_types='all'):
    """Imports video data for specified Hololive Production members.

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
        member_dir_path = df.SESSION_PATH / 'Video' / member_name
        if member_dir_path.exists():
            member_video_data[member_name] = _get_single_member_video_data(member_dir_path, video_data_types)

    return member_video_data


def _get_single_member_video_data(member_dir_path, video_data_types):
    single_member_video_data = {}

    for video_data_type in video_data_types:
        single_member_video_data[video_data_type] = _get_video_data(member_dir_path, video_data_type)

    return single_member_video_data


def _get_video_data(member_dir_path, video_data_type):
    data_file_path = None
    member_name = member_dir_path.name

    for file_path in member_dir_path.iterdir():
        if f'{member_name.lower()}_{video_data_type}' in file_path.name:
            data_file_path = file_path
            break

    if data_file_path is None:
        video_data = None
    else:
        video_data = pd.read_csv(data_file_path)

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
