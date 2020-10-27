# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for random number generator function."""

import pandas as pd
import pytest

from openclean.function.eval.random import Rand


@pytest.fixture
def dataset():
    """Simple dataset with one column and 10 rows."""
    data = [i for i in range(100)]
    return pd.DataFrame(
        data=data,
        columns=['A']
    )


def test_random_numbers(dataset):
    """Test the random number generator."""
    f = Rand(seed=4567)
    values = f.eval(dataset)
    assert len(values) == dataset.shape[0]
    for v in values:
        assert 0 <= v < 1
    f = Rand(seed=4567).prepare(['A'])
    for i in range(dataset.shape[0]):
        assert f([i]) == values[i]
