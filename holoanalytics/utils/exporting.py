"""Exporting of data

This module provides general-purpose tools relating the export of data for this project.

Functions:
    - create_directory: Creates a directory in the specified parent directory using the specified name.
    - create_session: Starts a new session.
    - export_dataframe: Exports a Pandas DataFrame.
"""

import os
from pathlib import Path
from datetime import datetime
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

    Currently, creates session for new data collection using the YouTube Data API. Future update will support sessions
    for other activities, including data collection using the Twitter and Twitch APIs.

    Returns:
        session_dir_path: Path object specifying the absolute path to the new session's directory.
    """

    session_id = 'YT-DAPI'
    sessions_path = df.YT_DAPI_SESSIONS_PATH

    session_dir_path = create_directory(sessions_path, f'{session_id}_Session', add_date=True)

    return session_dir_path


def export_dataframe(dataframe, dir_path, file_name, add_datetime=True, filetype='csv'):
    """Exports a Pandas DataFrame to a text file based on the specified file type, file name, and location.

    It is recommended to export the data to csv files.

    Args:
        dataframe: Pandas DataFrame to be exported.
        dir_path: Path object specifying the absolute path to the directory that the output file is to be created in.
        file_name: String specifying the name of the output file.
        add_datetime: Boolean specifying if datetime is added to the filename.
        filetype: String specifying the type of the output file. Default = 'csv'.

    Returns:
        None
    """

    if add_datetime is True:
        current_datetime = str(datetime.utcnow())
        current_date = current_datetime[0:10]
        current_time = current_datetime[11:16].replace(':', '')
        full_filename = f'{current_date}-{current_time}_{file_name}.{filetype}'
    else:
        full_filename = f'{file_name}.{filetype}'

    full_path = dir_path / full_filename
    dataframe.to_csv(full_path, index=False)

    print(f"'{full_filename}' has been successfully exported.")


def export_channel_data(data, export_data, filename):
    """Exports collected YouTube channel data.

    This function exports collected YouTube channel data to a text-based file located in the current session's
    'Channel' directory and with the specified file name.

    Args:
        data: Pandas DataFrame containing collected YouTube channel data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.
        filename: String specifying the name of the output file.

    Returns:
        None
    """

    if export_data is True and df.SESSION_PATH is not None:
        export_dataframe(data, df.SESSION_PATH / 'Channel', filename)


def export_video_data(member_name, data, export_data, filename):
    """Exports collected YouTube video data.

    This function exports collected YouTube video data to a text-based file located in the current session's
    'Video' directory and with the specified file name.

    Args:
        member_name: String specifying the name of the Hololive Production member whose videos data is being
                     collected for.
        data: Pandas DataFrame containing YouTube video data.
        export_data: Boolean specifying whether collected data is to be exported. Default = True.
        filename: String specifying the name of the output file.

    Returns:
        None
    """

    if export_data is True and df.SESSION_PATH is not None:
        member_name = member_name.replace(' ', '_')
        export_dataframe(data, df.SESSION_PATH / 'Video' / member_name, f'{member_name.lower()}_{filename}')
