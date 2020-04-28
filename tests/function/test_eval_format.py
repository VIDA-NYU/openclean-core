# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the split and format functions."""

import pandas as pd

from openclean.function.column import Col
from openclean.function.string import Format, Split


def test_format_column_values():
    """Test re-formating values from one or more columns in a data frame."""
    df = pd.DataFrame(
        data=[
            ['A.lice', '23'],
            ['B.ob', '33']
        ],
        columns=['A', 'B']
    )
    f = Format('{}{}', Split('A', '.')).prepare(df)
    assert f.exec(df.iloc[0]) == 'Alice'
    assert f.exec(df.iloc[1]) == 'Bob'
    f = Format('{}-{}', Col(['A', 'B'])).prepare(df)
    assert f.exec(df.iloc[0]) == 'A.lice-23'
    assert f.exec(df.iloc[1]) == 'B.ob-33'
    f = Format('Age {}', Col('B')).prepare(df)
    assert f.exec(df.iloc[0]) == 'Age 23'
    assert f.exec(df.iloc[1]) == 'Age 33'
