from holoanalytics.settings import session
from holoanalytics.settings import core as df
from holoanalytics.utils import setup, exporting
from holoanalytics.datacollection.youtube import youtube_api, channels, videos


def main():
    # Import Data
    starting_data = setup.import_data(df.STARTING_DATA_FILE)

    # Create New Session
    session.SESSION_PATH = exporting.create_session()

    # Setup for Data Collection
    client = youtube_api.initialize_api()

    # Collect YouTube Channel Data
    channel_data = channels.get_channel_data(client, starting_data.youtube_channel_id)

    # Collect YouTube Video Data
    video_data = videos.get_video_data(client, starting_data, channel_data['uploads_playlist_ids']['data'])

    # Close Client
    client.close()

    return channel_data, video_data


if __name__ == '__main__':
    member_channel_data, member_video_data = main()
