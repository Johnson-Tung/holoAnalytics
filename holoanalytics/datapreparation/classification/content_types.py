from datetime import datetime

import pandas as pd

from holoanalytics.datapreparation import reformatting as ref
from holoanalytics.utils import exporting


def classify_content_type(member_video_data, keyword_banks, export_data=True):
    """Determines the content type(s) for YouTube videos, using their video title keywords.

    Input data must include video title keywords returned by
    holoanalytics.datapreparation.extraction.video_keywords.extract_title_keywords().

    Args:
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing YouTube video data
                           for Hololive Production members, originally returned by
                           holoanalytics.utils.importing.import_video_data().
        keyword_banks: Dictionary of keyword banks, originally returned by
                       holoanalytics.utils.importing.import_keyword_banks().
        export_data: Boolean specifying if content type data is to be exported. Default = True.

    Returns:
        member_video_data: Dictionary of YouTube video data updated with content type data.
    """

    keyword_bank = ref.combine_keyword_banks(keyword_banks)

    for member_name, video_data in member_video_data.items():
        video_data['content_types'] = _classify_member_videos(member_name, video_data['video_title_keywords']['data'],
                                                              keyword_bank, export_data)

    return member_video_data


def _classify_member_videos(member_name, video_title_keywords, keyword_bank, export_data):
    video_data = {}

    video_ids = video_title_keywords['video_id']
    keyword_sets = video_title_keywords['title_keywords']
    all_results = []

    for keyword_set in keyword_sets:
        results = _check_keywords(keyword_set, keyword_bank)
        all_results.append(results)

    content_types = pd.Series(all_results, name='content_types')
    video_data['data'] = pd.concat([video_ids, content_types], axis=1)
    video_data['datetime'] = datetime.utcnow().replace(microsecond=0)

    if export_data is True:
        exporting.export_video_data(member_name, video_data['data'], 'content_types', video_data['datetime'])

    return video_data


def _check_keywords(keyword_set, keyword_bank):

    content_types = set()

    for extracted_keyword in keyword_set:
        for content_type, keywords in keyword_bank.items():
            if extracted_keyword in keywords:
                content_types.add(content_type)
                break

    return content_types
