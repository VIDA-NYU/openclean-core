# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the statistics evaluation functions."""

import pandas as pd
import pytest

from openclean.function.eval.aggregate import Avg, Count, Max, Min, Sum
from openclean.function.eval.base import Col


@pytest.fixture
def dataset():
    return pd.DataFrame(data=[[1, 2], [2, 3], [3, 4]], columns=['A', 'B'])


@pytest.mark.parametrize(
    'op,result',
    [(Avg, 3), (Max, 4), (Min, 2), (Sum, 9)]
)
def test_aggregate_function(dataset, op, result):
    """Compute min, max, and mean over values in a column."""
    assert op('B').eval(dataset) == [result] * dataset.shape[0]


def test_error_aggregate_prepare():
    """Test for RuntimeError when attempting to prepare a aggreagte function."""
    with pytest.raises(RuntimeError):
        Avg('A').prepare(['A'])


def test_nested_aggregate(dataset):
    """Test aggregate values from a nested evaluation function."""
    assert Count(Col('A') > Col('B')).eval(dataset) == [0] * dataset.shape[0]
    assert Count(Col('A') < Col('B')).eval(dataset) == [3] * dataset.shape[0]
