from holoanalytics import definitions as df
from holoanalytics.utils import setup, exporting
from holoanalytics.datacollection.youtube import youtube_api, channels, videos


def main():
    # Import Data
    starting_data = setup.import_data(df.STARTING_DATA_FILE)

    # Create New Session
    df.SESSION_PATH = exporting.create_session()

    # Setup for Data Collection
    client = youtube_api.initialize_api()

    # Collect YouTube Channel Data
    channel_data = channels.get_channel_data(client, starting_data.youtube_channel_id)

    # Collect YouTube Video Data
    videos.get_video_data(client, starting_data, channel_data['uploads_playlist_ids'])

    # Close Client
    client.close()


if __name__ == '__main__':
    main()
