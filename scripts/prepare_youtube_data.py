from holoanalytics.utils import importing
from holoanalytics.datapreparation.extraction.video_keywords import extract_title_keywords
from holoanalytics.datapreparation.classification.video_types import classify_video_type
from holoanalytics.datapreparation.classification.content_types import classify_content_type


def main():

    session_name = importing.request_session()
    importing.open_session(session_name)

    data = importing.import_video_data()

    data = extract_title_keywords(data)
    data = classify_video_type(data)
    data = classify_content_type(data)

    return data


if __name__ == '__main__':
    member_video_data = main()

