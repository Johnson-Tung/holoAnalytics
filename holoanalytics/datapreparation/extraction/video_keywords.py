import re

import pandas as pd

from holoanalytics.utils import exporting


eng_keyword_bank = {'Music Video': ['Official', 'Original', 'Cover', 'Covered', 'MV', 'AMV', '3DMV'],
                    'Karaoke': ['Karaoke', 'Singing', 'I tried singing', 'Song', 'Sing'],
                    'Chatting': ['Chat', 'Chatting', "Chattin'", "Let's Chat", 'CHIT-CHAT', 'Chit Chat', 'Talk',
                                 'FREE TALK', 'FREETALK', 'Zatsu', 'Zatsudan'],
                    'Watchalong': ['Watchalong', 'Watch-a-long', 'Watch-along'],
                    'Superchat Reading': ['Superchat Reading', 'SC Reading',  'SC Catchup', 'Supers',
                                          'Superchat', 'Superchats', 'Super Chat', 'Super Chats',
                                          'Supas', 'MCTHANKIES', 'SUPA Sunday', 'SUPASUNDAY', 'SUPASUNDAE',
                                          'Donation Reading'],
                    '3DLive': ['3DLIVE', '3D LIVE', 'Live'],
                    'Collab': ['Collab', 'Off Collab', 'Off Collaboration'],
                    'VR': ['VR', '3DVR', '360'],
                    'Drawing': ['Drawing', 'Draw', "Let's Draw"],
                    'Other Content': ['ASMR', 'Vlog', 'Animation', 'Manga Reading'],
                    'Debut': ['DEBUT STREAM', 'Debut', 'Self Introduction', 'First Live'],
                    'Outfit Reveal': ['New Outfit Reveal', 'New Outfit', 'Reveal'],
                    'Q&A': ['Q&A', 'Q & A', 'Marshmallow'],
                    'Review': ['Room Review'],
                    'Gaming': ['Minecraft', 'Mine craft', 'APEX', 'Abex', 'Elden Ring',
                               'AmongUs', 'Among Us', 'Tetris', 'Resident Evil',
                               'Pokemon', 'Pokémon',
                               'Mario', 'Mario Galaxy', 'Mario Party',
                               'Tetris99', 'Duolingo', 'Simulator', 'HoloCure', 'Rust', 'League of Legends',
                               'Ring Fit', 'RFA', 'Ring Fit Adventure', 'VALORANT', 'Raft',
                               'hololiveError', 'hololive ERROR']}
jp_keyword_bank = {'Music Video': ['公式', 'オリジナル曲'],  # Official, original song
                   'Karaoke': ['歌枠', '歌ってみた'],  # Song frame, I tried to sing
                   'Chatting': ['雑談'],  # Chatting
                   'Collab': ['オフコラボ'],  # Off collaboration
                   'Other Content': ['お絵かき'],  # Drawing
                   'Debut': ['初放送', '初配信', '初配神'],  # Debut
                   'Gaming': ['マイクラ', 'テトリス', 'テトリス99'],  # Minecraft, Tetris, Tetris99
                   'Other': ['告知']}  # Announcement
id_keywords = {}  # TODO: WIP

keyword_translated = {'歌枠': 'singing?', '歌ってみた': 'Tried Singing', '公式': 'Official', '雑談': 'Chat', '告知': 'Announcement',
                      '新衣装': 'New Costume', 'マイクラ': 'Minecraft', 'お絵かき': 'Drawing',
                      'ゲリラモンスターハンターライズ': 'Monster Hunter Rise?', 'テトリス99': ' Tetris99',
                      'オフコラボ': 'Off Collaboration', '初配神': 'Debut Stream?'}


def extract_title_keywords(member_video_data, keyword_banks, export_data=True):
    """Extracts keywords from YouTube video titles in the imported data.

    Args:
        member_video_data: Dictionary of dictionaries of Pandas DataFrames containing imported video data,
                           returned by holoanalytics.utils.importing.import_video_data().
        keyword_banks: Dictionary of keyword banks, originally returned by
                       holoanalytics.utils.importing.import_keyword_banks().
        export_data: Boolean specifying whether collected data is to be exported. Default = True.

    Returns:
        member_video_data: YouTube video data updated with video title keyword data.
    """
    search_keywords = {'English': unpack_keywords(keyword_banks.get('English', {})),
                       'Japanese': unpack_keywords(keyword_banks.get('Japanese', {})),
                       'Indonesian': unpack_keywords(keyword_banks.get('Indonesian', {}))}

    _check_search_keyword(search_keywords)

    for member_name, video_data in member_video_data.items():
        video_data['video_title_keywords'] = _extract_member_keywords(member_name, video_data['video_attributes'],
                                                                      search_keywords, export_data)

    return member_video_data


def _check_search_keyword(search_keywords):

    for value in search_keywords.values():
        if value:
            return

    raise ValueError("Provided keyword banks do not include keyword banks for any of the accepted languages.")


def _extract_member_keywords(member_name, video_attributes, search_keywords, export_data):
    """Extracts keywords from YouTube video titles for the specified Hololive Production member.

    Args:
        member_name: String specifying the name of the Hololive Production member whose video data is being prepared.
        video_attributes: Pandas DataFrame containing video attributes for the specified member's YouTube videos.
        eng_keywords: List of strings representing all keywords in the English word bank.
        jp_keywords: List of strings representing all keywords in the Japanese word bank.
        export_data: Boolean specifying whether collected data is to be exported.

    Returns:
        data: Pandas DataFrame containing video ids and corresponding video title keywords for the specified member.
    """

    titles = video_attributes['title']
    video_ids = video_attributes['video_id']

    all_results = []

    for title in titles:
        results = extract_bracketed_words(title)
        results |= extract_keywords(title, search_keywords['English'])
        results |= extract_keywords(title, search_keywords['Japanese'])
        results |= extract_keywords(title, search_keywords['Indonesian'])
        results |= extract_hashtags(title)
        all_results.append(results)

    title_keywords = pd.Series(all_results, name='title_keywords')
    data = pd.concat([video_ids, titles, title_keywords], axis=1)

    exporting.export_video_data(member_name, data, export_data, 'video_title_keywords')

    return data


def unpack_keywords(keyword_bank):
    """Unpack a keyword bank into a list of keywords.

    Args:
        keyword_bank: Dictionary of sets or lists with keyword group names as keys and keywords as values,
                      i.e. keyword_bank[keyword_group_name] = {keywords}.

    Returns:
        unpacked_keywords: List of strings representing keywords.
    """
    unpacked_keywords = []

    for keywords in keyword_bank.values():
        unpacked_keywords += list(keywords)

    return unpacked_keywords


def extract_bracketed_words(title):
    """Searches for and extracts words from YouTube video titles that are bound by brackets.

    Args:
        title: String representing the YouTube video title for which words will be extracted from.

    Returns:
        results: Set of strings representing bracketed words that have been found and extracted.
    """

    brackets = r'[【】≪≫『』「」\[\]\(\)]'
    pattern = re.compile(r'(【[^【】]*】)|(≪[^≪≫]*≫)|(『[^『』]*』)|(「[^「」]*」)|(\[[^\[\]]*\])|(\([^\(\)]*\))')

    matches = pattern.finditer(title)
    results = {re.sub(brackets, '', match.group()) for match in matches}

    return results


def extract_keywords(title, keywords):
    """Searches for and extracts specific keywords from YouTube video titles.

    Args:
        title: String representing the YouTube video title for which words will be extracted from.
        keywords: List of strings representing keywords to be searched for.

    Returns:
        results: Set of strings representing specific keywords that have been found and extracted.

    """

    results = set()

    for keyword in keywords:
        match = (re.search(rf'\b{keyword}\b', title, flags=re.IGNORECASE))
        # re.search returns 'None' if no match is found.
        if match is not None:
            results.add(match.group())

    return results


def extract_hashtags(title):
    """Searches for and extracts hashtags, e.g. '#Hashtag', from YouTube video titles.

    Args:
        title: String representing the YouTube video title for which words will be extracted from.

    Returns:
        results: Set of strings representing hashtags that have been found and extracted.
    """

    pattern = re.compile(r'#[A-Za-z0-9]*[A-Za-z]+')
    matches = pattern.finditer(title)
    results = {match.group() for match in matches}

    return results


def _check_data_format(titles):

    if isinstance(titles, str):
        titles = [titles]
    elif (isinstance(titles, list) or isinstance(titles, tuple) or isinstance(titles, set)
          or isinstance(titles, pd.Series)):
        pass
    else:
        raise TypeError("Input data uses an invalid data structure.")

    return titles

