# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the list evaluation functions. This also includes tests for
the column and constants evaluation funcitons.
"""

import pandas as pd

from openclean.function.eval.column import Col
from openclean.function.eval.constant import Const
from openclean.function.eval.aggregate.row import Max, Mean, Min, Sum


def test_scalar_list_functions():
    """Test computing min, max over scalar values from multiple columns in a
    data frame row.
    """
    df = pd.DataFrame(
        data=[
            [1, 2, 3],
            [4, 5, 6],
            [1, 4, 5]
        ],
        columns=['A', 'B', 'C']
    )
    LEAST = [1, 3, 1]
    GREATES = [3, 5, 4]
    AMOUNT = [6, 12, 8]
    MEAN = [2, 4, 2]
    least = Min('A', 'B', Const(3))
    greatest = Max('A', 'B', Const(3))
    mean = Mean('A', 'B', Const(3))
    amount = Sum('A', 'B', Const(3))
    least.prepare(df)
    greatest.prepare(df)
    mean.prepare(df)
    amount.prepare(df)
    for i in range(3):
        assert least.eval(df.iloc[i]) == LEAST[i]
        assert greatest.eval(df.iloc[i]) == GREATES[i]
        assert amount.eval(df.iloc[i]) == AMOUNT[i]
        assert int(mean.eval(df.iloc[i])) == MEAN[i]


def test_tuple_list_functions():
    """Test computing min, max over tuples from multiple columns in a data
    frame row.
    """
    df = pd.DataFrame(
        data=[
            [1, 2, 3],
            [4, 5, 6],
            [1, 4, 5]
        ],
        columns=['A', 'B', 'C']
    )
    LEAST = [(1, 2), (3, 3), (1, 4)]
    GREATES = [(3, 3), (5, 6), (4, 5)]
    values = [Col(['A', 'B']), Col(['B', 'C']), Const((3, 3))]
    least = Min(*values)
    greatest = Max(*values)
    least.prepare(df)
    greatest.prepare(df)
    for i in range(3):
        assert least.eval(df.iloc[i]) == LEAST[i]
        assert greatest.eval(df.iloc[i]) == GREATES[i]
