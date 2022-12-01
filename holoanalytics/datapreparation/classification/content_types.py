import pandas as pd

from holoanalytics.utils import exporting


def classify_content_type(member_video_data, keyword_bank, export_data=True):

    for member_name, video_data in member_video_data.items():
        video_data['content_type'] = _classify_member_videos(member_name, video_data['video_title_keywords'],
                                                             keyword_bank, export_data)

    return member_video_data


def _classify_member_videos(member_name, video_title_keywords, keyword_bank, export_data):

    video_ids = video_title_keywords['video_id']
    keyword_sets = video_title_keywords['title_keywords']
    all_results = []

    for keyword_set in keyword_sets:
        results = _check_keywords(keyword_set, keyword_bank)
        all_results.append(results)

    content_types = pd.Series(all_results, name='content_types')
    data = pd.concat([video_ids, content_types], axis=1)

    exporting.export_video_data(member_name, data, export_data, 'content_types')

    return content_types


def _check_keywords(keyword_set, keyword_bank):

    content_types = set()

    for extracted_keyword in keyword_set:
        for content_type, keywords in keyword_bank.items():
            if extracted_keyword in keywords:
                content_types.add(content_type)
                break

    return content_types
