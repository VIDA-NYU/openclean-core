# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for data utility functions."""

import pandas as pd
import pytest

from openclean.data.util import to_set


@pytest.fixture
def dataset():
    """Get simple dataset with the name and age of two people."""
    return pd.DataFrame(
        data=[['Alice', 29], ['Bob', 32]],
        columns=['Name', 'Age']
    )


def test_df_to_set(dataset):
    """Test converting a data frame with multiple columns into a set of
    tuples.
    """
    tuples = to_set(dataset[['Name', 'Age']])
    assert ('Alice', 29) in tuples
    assert ('Bob', 32) in tuples
    assert ('Alice', 32) not in tuples


def test_series_to_set(dataset):
    """Test converting a data series into a set of values."""
    names = to_set(dataset['Name'])
    assert 'Alice' in names
    assert 'Bob' in names
    assert 'Claire' not in names
