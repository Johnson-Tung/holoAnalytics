"""Exporting of data

This module provides general-purpose tools relating the export of data for this project.

Functions:
    - create_directory: Creates a directory in the specified parent directory using the specified name.
    - create_session: Starts a new session.
    - export_dataframe: Exports a Pandas DataFrame.
"""

import csv
from pathlib import Path
from datetime import datetime
import pandas as pd
from holoanalytics import definitions as df


def create_directory(parent_dir_path, dir_name, add_date=True, output_message=True):
    """Creates a directory in the specified parent directory using the specified name.

    Args:
        parent_dir_path: Path object specifying the absolute path to the directory that the new directory
                         is to be created in.
        dir_name: String specifying the name of the directory to be created.
        add_date: Boolean specifying if the date is to be added to the directory name. Default = True.
        output_message: Boolean specifying if an output message is to be printed. Default = True.

    Returns:
        full_path: Path object specifying the absolute path to the created directory.
    """

    # If 'add_data' is overwritten with any other value, assume date is not to be included.
    # Add date only if default value is used.
    if add_date is True:
        current_date = f'{str(datetime.utcnow())[0:10]}'    # Format: YYYY-MM-DD
        dir_name = f'{current_date}_{dir_name}'

    full_path = parent_dir_path / dir_name

    if full_path.exists():
        full_path = _adjust_file_path(full_path)

    full_path.mkdir(parents=True)

    if output_message is True:
        dir_name = full_path.name
        parent_dir_name = parent_dir_path.name
        print(f"'{dir_name}' has been successfully created in '{parent_dir_name}'.")

    return full_path


def _adjust_file_path(old_path):
    """Adjusts the current absolute path, if it is taken, by attaching a number at the end, e.g. 'Path-2'

    PRIVATE function.

    Args:
        old_path: Path object specifying the original absolute path.

    Returns:
        new_path: Path object specifying the new absolute path
    """

    counter = 2

    while True:
        new_path = Path(f'{old_path}-{counter}')
        if not new_path.exists():
            break
        counter += 1

    return new_path


def create_session():
    """Starts a new session and creates a directory to store all new files that will be created during said session.

    Returns:
        session_dir_path: Path object specifying the absolute path to the new session's directory.
    """

    session_id = 'YT-DAPI'
    sessions_path = df.YT_DAPI_SESSIONS_PATH

    session_dir_path = create_directory(sessions_path, f'{session_id}_Session', add_date=True)

    df.SESSION_PATH = session_dir_path

    return session_dir_path


def export_dataframe(dataframe, dir_path, file_name, timestamp=None, auto_timestamp=True, filetype='csv'):
    """Exports a Pandas DataFrame to a text file based on the specified file type, file name, and location.

    It is recommended to export the data to csv files.

    When including a timestamp in the file name, non-None arguments for 'timestamp' will take priority over
    'auto_timestamp', i.e. A timestamp that is manually provided will be used, even if an automatic timestamp was
    requested.

    Args:
        dataframe: Pandas DataFrame to be exported.
        dir_path: Path object specifying the absolute path to the directory that the output file is to be created in.
        file_name: String specifying the name of the output file.
        timestamp: Datetime object or Pandas Timestamp that specifies the date and time to be used in the output file
                   name, i.e.the timestamp associated with the collection or preparation of the DataFrame's data.
                   Default = None.
        auto_timestamp: Boolean specifying if a timestamp using the current date and time is to be automatically
                        added to the file name. Default = False.
        filetype: String specifying the type of the output file. Default = 'csv'.

    Returns:
        None
    """

    if timestamp is None and auto_timestamp is False:
        full_filename = f'{file_name}.{filetype}'
    else:
        # Check if a valid timestamp was specified.
        if isinstance(timestamp, (datetime, pd.Timestamp)):
            pass
        elif timestamp is not None:
            raise TypeError(f"'date_time' needs to be a Datetime object, not a '{type(timestamp)}' object.")
        # No timestamp was specified, so check if a timestamp was requested to be added automatically.
        elif auto_timestamp is True:
            timestamp = str(datetime.utcnow())
        else:
            raise TypeError(f"'add_datetime' needs to be a Boolean, not a '{type(auto_timestamp)}' object.")

        date = timestamp[0:10]
        time = timestamp[11:16].replace(':', '')
        full_filename = f'{date}-{time}_{file_name}.{filetype}'

    full_path = dir_path / full_filename
    dataframe.to_csv(full_path, index=False)

    print(f"'{full_filename}' has been successfully exported.")


def export_channel_data(data, filename, timestamp=None, auto_timestamp=True, filetype='csv'):
    """Exports YouTube channel data.

    This function exports collected or prepared YouTube channel data to a text-based file located in the current
    session's 'Channel' directory and with the specified file name.

    When including a timestamp in the file name, non-None arguments for 'timestamp' will take priority over
    'auto_timestamp', i.e. A timestamp that is manually provided will be used, even if an automatic timestamp was
    requested.

    Args:
        data: Pandas DataFrame containing YouTube channel data.
        export_data: Boolean specifying whether the data is to be exported. Default = True.
        filename: String specifying the name of the output file.
        timestamp: Datetime object or Pandas Timestamp that specifies the date and time to be used in the output file
                   name, i.e.the timestamp associated with the collection or preparation of the DataFrame's data.
                   Default = None.
        auto_timestamp: Boolean specifying if a timestamp using the current date and time is to be automatically
                        added to the file name. Default = False.
        filetype: String specifying the type of the output file. Default = 'csv'.

    Returns:
        None
    """

    if df.SESSION_PATH is None:
        df.SESSION_PATH = create_session()

    dir_path = df.SESSION_PATH / 'Channel'

    if not dir_path.exists():
        dir_path = create_directory(dir_path.parent, dir_path.name, add_date=False)

    export_dataframe(data, dir_path, filename, timestamp, auto_timestamp, filetype)


def export_video_data(member_name, data, filename, timestamp=None, auto_timestamp=True, filetype='csv'):
    """Exports YouTube video data.

    This function exports collected or prepared YouTube video data to a text-based file located in the current
    session's 'Video' directory and with the specified file name.

    When including a timestamp in the file name, non-None arguments for 'timestamp' will take priority over
    'auto_timestamp', i.e. A timestamp that is manually provided will be used, even if an automatic timestamp was
    requested.

    Args:
        member_name: String specifying the name of the Hololive Production member that the video data belongs to.
        data: Pandas DataFrame containing YouTube video data.
        export_data: Boolean specifying whether the data is to be exported. Default = True.
        filename: String specifying the name of the output file.
        timestamp: Datetime object or Pandas Timestamp that specifies the date and time to be used in the output file
                   name, i.e.the timestamp associated with the collection or preparation of the DataFrame's data.
                   Default = None.
        auto_timestamp: Boolean specifying if a timestamp using the current date and time is to be automatically
                        added to the file name. Default = False.
        filetype: String specifying the type of the output file. Default = 'csv'.

    Returns:
        None
    """

    if df.SESSION_PATH is None:
        df.SESSION_PATH = create_session()

    member_name = member_name.replace(' ', '_')
    dir_path = df.SESSION_PATH / 'Video' / member_name

    if not dir_path.exists():
        dir_path = create_directory(dir_path.parent, dir_path.name, add_date=False)

    export_dataframe(data, dir_path, f'{member_name.lower()}_{filename}', timestamp, auto_timestamp, filetype)


def export_keyword_banks(keyword_banks):
    """Exports keywords banks to .csv files.

    Args:
        keyword_banks: Dictionary of keyword banks, originally returned by
                       holoanalytics.utils.importing.import_keyword_banks().

    Returns:
        None
    """

    for language, keyword_bank in keyword_banks.items():

        file_path = df.KEYWORD_BANKS_PATH / f'{language.lower()}_video_title_keywords.csv'

        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for keyword_group, keywords in keyword_bank.items():
                writer.writerow([keyword_group] + list(keywords))

