# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the statistics evaluation functions."""

import pandas as pd

from openclean.function.statistics import Max, Mean, Min


def test_statistics_functions():
    """Compute min, max, and mean over values in a column."""
    df = pd.DataFrame(data=[[1, 2], [2, 3], [3, 4]], columns=['A', 'B'])
    funcs = [Min('B'), Mean('B'), Max('B')]
    # Prepare all functions.
    for f in funcs:
        f.prepare(df)
    results = [set(), set(), set()]
    for _, values in df.iterrows():
        for i in range(3):
            results[i].add(funcs[i].exec(values))
    for r in results:
        assert len(r) == 1
    assert list(results[0])[0] == 2
    assert list(results[1])[0] == 3
    assert list(results[2])[0] == 4
