# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Fixtures for engine database unit tests."""

import pandas as pd
import pytest


@pytest.fixture
def dataset():
    """Returns a basic data frame with three columns and two rows."""
    return pd.DataFrame(data=[[1, 2, 3], [3, 4, 5]], columns=['A', 'B', 'C'])
