from pathlib import Path
import pandas as pd
from holoanalytics import definitions as df

VIDEO_DATA_TYPES = ('video_attributes', 'video_stats')


def open_session(session):
    df.SESSION_PATH = Path(
        df.PROJECT_ROOT / 'results' / 'collected' / 'YouTube' / 'YouTube-Data-API-Sessions' / session)


def import_member_data(member_names):
    member_data = {}

    for member_name in member_names:
        member_name = member_name.replace(' ', '_')
        member_dir_path = df.SESSION_PATH / 'Video' / member_name
        member_data[member_name] = import_all_video_data(member_dir_path)

    return member_data


def import_all_video_data(member_dir_path):
    data = {}

    for data_type in VIDEO_DATA_TYPES:
        data[data_type] = import_video_data(data_type, member_dir_path)

    return data


def import_video_data(data_type, member_dir_path):
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
