# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper functions for parallel processing."""

from typing import Callable, List

import multiprocessing as mp


def process_list(func: Callable, values: List, processes: int) -> List:
    """Process a given list of values in parallel. Applies the given function
    to each value in the list and returnes the processed result.

    The current implementation uses a multiprocess pool to process the list
    with the default map function. In the future we may want to modify this
    behavior, e.g., use imap to account for large lists.

    Parameters
    ----------
    func: callable
        Function that is applied to list values.
    values: list
        List of values that are processed by the given function.
    processes: int
        Number of parallel proceses to use.

    Returns
    -------
    list
    """
    return mp.Pool(processes=processes).map(func, values)