# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for data type detection predicates."""

from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from openclean.function.eval.datatype import IsDatetime, IsInt, IsFloat, IsNaN
from openclean.function.eval.datatype import Bool, Datetime, Int, Float


TS = datetime(2020, 5, 1)


@pytest.fixture
def dataset():
    """Get a data frame wit a single row that has values of different data
    types: datetime (in column 'A'), int ('B'), float ('C'), np.nan ('D'),
    string ('E'), bool ('F'), and None ('G').
    """
    return pd.DataFrame(
        data=[[TS, 1, 23.45, np.nan, 'ABC', False, None]],
        columns=['A', 'B', 'C', 'D', 'E', 'F', 'G']
    )


@pytest.mark.parametrize(
    'op,col,result',
    [
        (Datetime, 'A', TS),
        (Int, 'B', 1),
        (Float, 'C', 23.45),
        (Bool, 'E', True),
        (Bool, 'F', False),
        (Bool, 'G', False)
    ]
)
def test_datatype_cast(op, col, result, dataset):
    """Test casting the value in a data frame column to a given type."""
    assert op(col).eval(dataset) == [result]
    assert op(col).prepare(dataset.columns)(dataset.iloc[0]) == result


@pytest.mark.parametrize(
    'op,col',
    [(IsDatetime, 'A'), (IsInt, 'B'), (IsFloat, 'C'), (IsNaN, 'D')]
)
def test_datatype_predicate(op, col, dataset):
    """Test data type detection predicates for values from a single column."""
    assert op(col).eval(dataset) == [True]
    assert op(col).prepare(dataset.columns)(dataset.iloc[0]) == True
    # Column 'E' contains a string value that cannot be converted to any of the
    # tested data types.
    assert op('E').eval(dataset) == [False]
    assert op('E').prepare(dataset.columns)(dataset.iloc[0]) == False
