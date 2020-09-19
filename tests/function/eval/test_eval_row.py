# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for row aggregation functions."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col, Cols
from openclean.function.eval.row import Greatest, Least


@pytest.fixture
def dataset():
    return pd.DataFrame(
        data=[
            [1, 2, 3],
            [4, 5, 6],
            [1, 4, 5]
        ],
        columns=['A', 'B', 'C']
    )


@pytest.mark.parametrize(
    'op,results',
    [(Least, [1, 3, 1]), (Greatest, [3, 5, 4])]
)
def test_aggregate_over_scalar_values(dataset, op, results):
    """Test computing min and max over scalar values from multiple columns in a
    data frame row.
    """
    f = op(Col('A'), Col('B'), 3).prepare(dataset)
    for i in range(3):
        assert f.eval(dataset.iloc[i]) == results[i]


@pytest.mark.parametrize(
    'op,results',
    [(Least, [[1, 2], [3, 3], [1, 4]]), (Greatest, [[3, 3], [5, 6], [4, 5]])]
)
def test_aggregate_over_list_values(dataset, op, results):
    """Test computing min and max over lists of values from multiple columns
    in a data frame row.
    """
    f = op(Col(['A', 'B']), Col(['B', 'C']), [3, 3]).prepare(dataset)
    for i in range(3):
        assert f.eval(dataset.iloc[i]) == results[i]


@pytest.mark.parametrize(
    'op,results',
    [(Least, [(1, 2), (3, 3), (1, 4)]), (Greatest, [(3, 3), (5, 6), (4, 5)])]
)
def test_aggregate_over_tuple_values(dataset, op, results):
    """Test computing min and max over tuples of values from multiple columns
    in a data frame row.
    """
    f = op(Cols('A', 'B'), Cols('B', 'C'), (3, 3)).prepare(dataset)
    for i in range(3):
        assert f.eval(dataset.iloc[i]) == results[i]