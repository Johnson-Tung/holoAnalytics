from holoanalytics.utils import setup
from holoanalytics.utils import importing
from holoanalytics.datapreparation.extraction.video_keywords import extract_title_keywords
from holoanalytics.datapreparation import reformatting as reform
from holoanalytics.datapreparation.classification.video_types import classify_video_type
from holoanalytics.datapreparation.classification.content_types import classify_content_type
from holoanalytics.datapreparation.coalescence import coalesce_video_data
from holoanalytics.datapreparation import summary


def main():

    starting_data = importing.import_member_data()

    session_name = setup.request_session()
    setup.open_session(session_name)

    channel_data = importing.import_channel_data()

    video_data = importing.import_video_data()
    keyword_banks = importing.import_keyword_banks('english', 'japanese', 'indonesian')

    video_data = reform.reformat_datetimes(None, video_data)

    video_data = extract_title_keywords(video_data, keyword_banks)
    video_data = classify_video_type(video_data)
    video_data = classify_content_type(video_data, keyword_banks)
    video_data = coalesce_video_data(starting_data, video_data)

    channel_data = summary.summarize_video_data(video_data, channel_data)
    channel_data = summary.summarize_by_unit(starting_data, channel_data)

    return channel_data, video_data


if __name__ == '__main__':
    member_channel_data, member_video_data = main()

