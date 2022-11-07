from holoanalytics import definitions as df
from holoanalytics.utils import exporting
from holoanalytics.utils import setup
from holoanalytics.datacollection.youtube import youtube_api, videos
from holoanalytics.datacollection.youtube import channels


def main():
    # Import Data
    starting_data = setup.import_data(df.STARTING_DATA_FILE)

    # Create New Session
    df.SESSION_PATH = exporting.create_session()

    # Setup for Data Collection
    client = youtube_api.initialize_api()

    # Collect YouTube Channel Data
    _, _, _, uploads_playlist_ids = channels.get_channel_data(client, starting_data.youtube_channel_id)

    # Collect YouTube Video Data
    videos.get_video_data(client, starting_data, uploads_playlist_ids)

    # Close Client
    client.close()


if __name__ == '__main__':
    main()
