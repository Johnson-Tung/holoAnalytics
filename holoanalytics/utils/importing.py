import csv
import pandas as pd
from holoanalytics import definitions as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats',  # Collected
                    'content_types', 'video_title_keywords', 'video_types')  # Prepared
CHANNEL_DATA_TYPES = ('channel_stats', 'channel_thumbnail_urls', 'channel_titles', 'uploads_playlist_ids',  # Collected
                      'channel_video_summary')  # Prepared


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


def import_channel_data(channel_data_types='all'):

    channel_data_types = _check_data_subtypes('channel', channel_data_types)
    member_channel_data = _member_channel_data(channel_data_types)

    return member_channel_data


def _member_channel_data(channel_data_types):
    member_channel_data = {}

    dir_path = df.SESSION_PATH / 'Channel'

    if not dir_path.exists():
        raise FileNotFoundError("This session does not have a 'Channel' data folder.")

    file_paths = list(dir_path.iterdir())
    file_paths.reverse()  # Get file paths from latest to earliest.

    for channel_data_type in channel_data_types:
        for file_path in file_paths:
            if f'{channel_data_type}' in file_path.name:
                headers = [0, 1] if channel_data_type == 'channel_video_summary' else [0]  # Note: Temporary solution.
                member_channel_data[channel_data_type] = pd.read_csv(file_path, header=headers)
                break

    return member_channel_data


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

    member_video_data = _member_video_data(member_names, video_data_types)

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
    elif isinstance(data_subtypes, list) or isinstance(data_subtypes, tuple) or isinstance(data_subtypes, pd.Series):
        pass
    else:
        raise ValueError

    return data_subtypes


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
