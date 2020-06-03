# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for type casting of column values."""

import pandas as pd
import pytest

from openclean.function.column import Col


def test_column_cast_functions():
    """Test casting values extracted from a data frame column."""
    df = pd.DataFrame(
        data=[
            [1, '2', 3],
            ['4', '5', 6],
            ['1', 'X', 5]
        ],
        columns=['A', 'B', 'C']
    )
    # No type casting
    col = Col('B')
    col.prepare(df)
    col.eval(df.iloc[0]) == '2'
    # No type casting
    col = Col('B', as_type=int)
    col.prepare(df)
    col.eval(df.iloc[0]) == 2
    # Default values
    col = Col('B', as_type=int, default_value=-1)
    col.prepare(df)
    col.eval(df.iloc[0]) == 2
    col.eval(df.iloc[2]) == -1
    # Raise error
    col = Col('B', as_type=int, raise_error=True)
    col.prepare(df)
    col.eval(df.iloc[0]) == 2
    with pytest.raises(ValueError):
        col.eval(df.iloc[2])
    # Variable cast
    col = Col(['A', 'B'])
    col.prepare(df)
    col.eval(df.iloc[0]) == (1, '2')
    col = Col(['A', 'B'], as_type=int)
    col.prepare(df)
    col.eval(df.iloc[0]) == (1, 2)
