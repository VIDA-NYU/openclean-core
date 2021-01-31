# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for is empty predicates for data frame rows."""

import pandas as pd
import pytest

from openclean.function.eval.null import IsEmpty, IsNotEmpty


@pytest.fixture
def dataset():
    """Simple dataset with two columns and some None values."""
    return pd.DataFrame(
        data=[['A', None], [None, 'B'], ['C', 'D']],
        columns=['A', 'B']
    )


@pytest.mark.parametrize(
    'op,result',
    [(IsEmpty, [False, True, False]), (IsNotEmpty, [True, False, True])]
)
def test_null_predicates(op, result, dataset):
    """Test is empty predicate."""
    assert op('A').eval(dataset) == result
    f = op('A').prepare(dataset.columns)
    assert [f(row) for row in dataset.itertuples(index=False, name=None)] == result
