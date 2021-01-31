# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for data utility functions."""

import pandas as pd
import pytest

from openclean.data.util import repair_mapping, to_set


@pytest.fixture
def dataset():
    """Get simple dataset with the name and age of two people."""
    return pd.DataFrame(
        data=[['Alice', 29], ['Bob', 32]],
        columns=['Name', 'Age']
    )


# -- repair_lookup ------------------------------------------------------------

def test_repair_mapping():
    """Test the repair lookup generator function."""
    df = pd.DataFrame(data=[[1, 2, 3], [1, 1, 4]], columns=['A', 'B', 'C'])
    assert repair_mapping(df, key='A', value='B') == {1: 2}
    assert repair_mapping(df, key=['A', 'B'], value='C') == {(1, 2): 3, (1, 1,): 4}
    # Error if no valid mapping exists.
    with pytest.raises(ValueError):
        repair_mapping(df, key='A', value='C')


# -- to_set -------------------------------------------------------------------

def test_df_to_set(dataset):
    """Test converting a data frame with multiple columns into a set of
    tuples.
    """
    names = to_set(dataset[['Name']])
    assert len(names) == 2
    assert 'Alice' in names
    assert 'Bob' in names
    tuples = to_set(dataset[['Name', 'Age']])
    assert ('Alice', 29) in tuples
    assert ('Bob', 32) in tuples
    assert ('Alice', 32) not in tuples


def test_invalid_to_set():
    """Test error when invalid object is passed to the to_set function."""
    with pytest.raises(ValueError):
        to_set(['A', 'B'])


def test_series_to_set(dataset):
    """Test converting a data series into a set of values."""
    names = to_set(dataset['Name'])
    assert 'Alice' in names
    assert 'Bob' in names
    assert 'Claire' not in names
