# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the apply operator."""

import numpy as np

from openclean.function.value.datatype import is_nan
from openclean.function.value.mapping import replace
from openclean.function.value.normalize import DivideByTotal
from openclean.function.value.string import to_upper
from openclean.operator.transform.apply import apply
from openclean.operator.transform.update import update


NAMES_UPPER = ['ALICE', 'BOB', 'CLAUDIA', 'DAVE', 'EILEEN', 'FRANK', 'GERTRUD']


def test_simple_apply_operator(employees):
    """Test applying a single callabel to columns of a data frame."""
    df = apply(employees, 'Name', to_upper)
    assert list(df['Name']) == NAMES_UPPER
    assert df['Salary'][2] == '21k'
    df = apply(employees, df.columns, to_upper)
    assert list(df['Name']) == NAMES_UPPER
    assert df['Salary'][2] == '21K'


def test_apply_factory_operator(employees):
    """Test apply factory pattern for value normalization."""
    # If one value in the column is NaN the normalization result is zero for
    # all values.
    df = apply(employees, 'Age', DivideByTotal())
    for v in df['Age']:
        assert v == 0 or is_nan(v)
    df = apply(apply(employees, 'Age', np.nan_to_num), 'Age', DivideByTotal())
    for v in df['Age'].iloc[[0, 1, 2, 4, 5, 6]]:
        assert v > 0
    # Same results as before but using conditional replace
    df = update(employees, 'Age', replace(is_nan, 0))
    df = apply(df, 'Age', DivideByTotal())
    for v in df['Age'].iloc[[0, 1, 2, 4, 5, 6]]:
        assert v > 0
    # Test another shortcut
    df = update(employees, 'Age', replace(is_nan, 0))
    df = apply(df, 'Age', DivideByTotal())
    for v in df['Age'].iloc[[0, 1, 2, 4, 5, 6]]:
        assert v > 0
