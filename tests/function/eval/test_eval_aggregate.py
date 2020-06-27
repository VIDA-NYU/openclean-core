# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the statistics evaluation functions."""

import pandas as pd
import pytest

from openclean.function.eval.aggregate import Avg, Max, Min, Sum


@pytest.fixture
def dataset():
    return pd.DataFrame(data=[[1, 2], [2, 3], [3, 4]], columns=['A', 'B'])


@pytest.mark.parametrize(
    'op,result',
    [(Avg, 3), (Max, 4), (Min, 2), (Sum, 9)]
)
def test_statistics_functions(dataset, op, result):
    """Compute min, max, and mean over values in a column."""
    f = op('B').prepare(dataset)
    for _, values in dataset.iterrows():
        assert f.eval(values) == result
