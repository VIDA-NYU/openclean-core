# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for regular expression match predicates for data frame rows."""

import pandas as pd
import pytest

from openclean.function.eval.regex import IsMatch, IsNotMatch


@pytest.fixture
def dataset():
    """Simple dataset with the name and age of four people."""
    return pd.DataFrame(
        data=[
            ['Alice', 45],
            ['Bob', 23],
            ['Claudia', 25],
            ['Rob', 56]
        ],
        columns=['Name', 'Age']
    )


@pytest.mark.parametrize(
    'op,result',
    [
        (IsMatch, [False, True, False, True]),
        (IsNotMatch, [True, False, True, False])
    ]
)
def test_predicate_regex(op, result, dataset):
    """Test is match and not is match predicates."""
    assert op(pattern='.+ob', columns='Name').eval(dataset) == result
    f = op(pattern='.+ob', columns='Name').prepare(dataset.columns)
    assert [f(row) for row in dataset.itertuples(index=False, name=None)] == result
