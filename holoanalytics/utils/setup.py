"""Project setup

This module provides tools to prepare for data collection, transformation, and visualization.

Functions:
    - import_data: Imports data from a csv file into a Pandas DataFrame.
    - config_pd_options: Configures Pandas display settings.
"""

import pandas as pd


def import_data(csv_file):
    """Imports data from a csv file into a Pandas DataFrame.

    Args:
        csv_file: Path object or string specifying the absolute path to the csv file.

    Returns:
        data: Pandas Dataframe containing imported data.
    """

    data = pd.read_csv(csv_file)
    print("Data has been successfully loaded")

    return data


def config_pd_options():
    """Configures Pandas display settings.

    Returns:
        None
    """

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 0)
    pd.set_option('display.max_colwidth', None)




