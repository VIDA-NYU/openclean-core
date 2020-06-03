# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the split and format functions."""

import pandas as pd

from openclean.function.column import Col
from openclean.function.string import Capitalize, Format, Lower, Upper


def test_format_column_values():
    """Test re-formating values from one or more columns in a data frame."""
    df = pd.DataFrame(
        data=[
            ['A.lice', 23],
            ['B.ob', 33]
        ],
        columns=['A', 'B']
    )

    def split(value):
        return value[0].split('.')

    f = Format('{}{}', split).prepare(df)
    assert f.eval(df.iloc[0]) == 'Alice'
    assert f.eval(df.iloc[1]) == 'Bob'
    f = Format('{}-{}', Col(['A', 'B'])).prepare(df)
    assert f.eval(df.iloc[0]) == 'A.lice-23'
    assert f.eval(df.iloc[1]) == 'B.ob-33'
    f = Capitalize('A').prepare(df)
    assert f.eval(df.iloc[0]) == 'A.lice'
    assert f.eval(df.iloc[1]) == 'B.ob'
    f = Lower('A').prepare(df)
    assert f.eval(df.iloc[0]) == 'a.lice'
    assert f.eval(df.iloc[1]) == 'b.ob'
    f = Upper('B', as_string=True).prepare(df)
    assert f.eval(df.iloc[0]) == '23'
    assert f.eval(df.iloc[1]) == '33'
