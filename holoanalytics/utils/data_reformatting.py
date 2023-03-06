"""Reformatting of data

This module provides tools to reformat data into formats that are compatible with features in this project.

Functions:
    - split_n_sized_batches: Splits data into batch(es) containing n-number of values where possible.
"""

import math
import pandas as pd


def split_n_sized_batches(data, batch_size):
    """Splits a sequence of values into a list of one or more lists containing n-number of values where possible.

    Example: Sequence of 12 integers with a batch size of 5. The result will be a list of 3 lists: 2
             lists containing 5 integers and a 3rd list containing 2 integers.

    Args:
        data: Sequence, e.g. Pandas Series, of values.
        batch_size: Integer representing the ideal number ("n") of values to be included a single batch.

    Returns:
        batched_data: List of lists where each list contains n-values where possible and the final list contains
                      any remaining values.
    """

    batched_data = []

    # Ensure results structure of 'results' is valid, i.e. list or tuple
    data = _confirm_list_or_tuple(data)

    for batch in range(1, math.ceil(len(data) / batch_size) + 1):
        # Check if there are enough items remaining for a full batch
        if len(data) - (batch * batch_size) < 0:  # No
            batched_data.append(data[(batch - 1) * batch_size:len(data)])
        else:  # Yes
            batched_data.append(data[(batch - 1) * batch_size:batch * batch_size])

    return batched_data


def _confirm_list_or_tuple(data):
    """Checks if the input data is contained in a list or tuple and tries to make corrections if necessary.

    If the input data is in a list or tuple, no changes are needed.
    If the input data is in a Panda Series or set, data structure will be changed.
    If the input data is a single string, it will be placed in a list by itself.

    PRIVATE function.

    Args:
        data: Sequence of values contained in a list, tuple, set, Pandas Series, etc.

    Returns:
        data: Sequence of values contained in a list or tuple.
    """

    if isinstance(data, (list, tuple)):
        pass
    elif isinstance(data, (pd.Series, set, range)):
        data = list(data)
    elif isinstance(data, str):
        data = [data]
    else:
        raise TypeError("Input data uses an invalid data structure.")

    return data

