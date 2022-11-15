import re

import pandas as pd

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


def unpack_keywords(language):
    """Imports the bank of keywords for the specified language and unpacks it into a single list.

    Args:
        language: String specifying the language of the keywords to be searched for, i.e. 'english' and 'japanese'.

    Returns:
        unpacked_keywords: List of strings representing all keywords for the specified language.
    """

    unpacked_keywords = []

    if language == 'english':
        for keywords in eng_keyword_bank.values():
            unpacked_keywords += keywords
    elif language == 'japanese':
        for keywords in jp_keyword_bank.values():
            unpacked_keywords += keywords
    else:
        raise ValueError('Unsupported language. Only English and Japanese are currently supported.')

    return unpacked_keywords


def extract_title_keywords(video_ids, titles):
    """Extracts keywords from YouTube video titles.

    Args:
        video_ids: Sequence, e.g. list, of strings representing the YouTube video ids for which
                   keywords will be extracted from.
        titles: Sequence, e.g. list, of strings representing the YouTube video titles for which keywords will be
                extracted from.

    Returns:
        data: Pandas DataFrame containing video ids, video titles, and extracted title keywords
    """

    all_results = []
    eng_search_keywords = unpack_keywords('english')
    jp_search_keywords = unpack_keywords('japanese')

    titles = _check_data_format(titles)

    for title in titles:
        results = extract_bracketed_words(title)
        results |= extract_keywords(title, eng_search_keywords)
        results |= extract_keywords(title, jp_search_keywords)
        results |= extract_hashtags(title)
        all_results.append(results)

    title_keywords = pd.Series(all_results, name='title_keywords')

    data = pd.concat([video_ids, titles, title_keywords], axis=1)

    return data


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

