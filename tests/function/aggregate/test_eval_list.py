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

from openclean.function.column import Col
from openclean.function.constant import Const
from openclean.function.aggregate.row import Greatest, Least


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
    values = [Col('A'), Col('B'), Const(3)]
    least = Least(values)
    greatest = Greatest(values)
    least.prepare(df)
    greatest.prepare(df)
    for i in range(3):
        assert least.eval(df.iloc[i]) == LEAST[i]
        assert greatest.eval(df.iloc[i]) == GREATES[i]


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
    least = Least(values)
    greatest = Greatest(values)
    least.prepare(df)
    greatest.prepare(df)
    for i in range(3):
        assert least.eval(df.iloc[i]) == LEAST[i]
        assert greatest.eval(df.iloc[i]) == GREATES[i]
