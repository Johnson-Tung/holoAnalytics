"""Project setup

This module provides tools to prepare for data collection, transformation, and visualization.

Functions:
    - import_data: Imports data from a csv file into a Pandas DataFrame.
    - config_pd_options: Configures Pandas display settings.
"""

import pandas as pd

from holoanalytics.settings import core, session


def import_data(csv_file):
    """Imports data from a csv file into a Pandas DataFrame.

    Args:
        csv_file: Path object or string specifying the absolute path to the csv file.

    Returns:
        data: Pandas Dataframe containing imported data.
    """

    data = pd.read_csv(csv_file)
    print("Data has been successfully loaded")

    return data


def config_pd_options():
    """Configures Pandas display settings.

    Returns:
        None
    """

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 0)
    pd.set_option('display.max_colwidth', None)


def request_session(session_open=True):
    """Displays available sessions and allows the user to request a specific session.

    Args:
        session_open: Boolean specifying if the requested session is to be opened. Default = True.

    Returns:
        session_name: String specifying the name of the requested session.
    """

    session_dir_paths = list(core.YT_DAPI_SESSIONS_PATH.iterdir())
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
        session_name: String specifying the data collection session to be opened.

    Returns:
        None
    """

    session.SESSION_PATH = core.YT_DAPI_SESSIONS_PATH / session_name
