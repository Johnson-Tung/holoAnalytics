from holoanalytics.utils import importing
from holoanalytics.datapreparation.extraction.video_keywords import extract_title_keywords
from holoanalytics.datapreparation import reformatting as ref
from holoanalytics.datapreparation.classification.video_types import classify_video_type
from holoanalytics.datapreparation.classification.content_types import classify_content_type
from holoanalytics.datapreparation import summary


def main():

    session_name = importing.request_session()
    importing.open_session(session_name)

    data = importing.import_video_data()
    keyword_banks = importing.import_keyword_banks('english', 'japanese', 'indonesian')

    data = ref.reformat_video_data(data)

    data = extract_title_keywords(data, keyword_banks)
    data = classify_video_type(data)
    data = classify_content_type(data, keyword_banks)

    channel_data = summary.summarize_video_data(data)

    return channel_data, data


if __name__ == '__main__':
    member_channel_data, member_video_data = main()

