"""Paths to key directories

This module defines absolute paths to key directories using Path objects.

"""

from pathlib import Path

SOURCE_ROOT = Path(__file__).parent.parent
PROJECT_ROOT = SOURCE_ROOT.parent
STARTING_DATA_FILE = SOURCE_ROOT / 'data' / 'starting' / 'holopro_members.csv'
KEYWORD_BANKS_PATH = SOURCE_ROOT / 'data' / 'keyword_banks'
MEMBER_PLOT_COLOURS_PATH = SOURCE_ROOT / 'data' / 'misc' / 'member_plot_colours.csv'
RESULTS_PATH = PROJECT_ROOT / 'results'
YT_DAPI_SESSIONS_PATH = RESULTS_PATH / 'data' / 'YouTube' / 'YouTube-Data-API-Sessions'
