# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Fixtures for pipeline unit tests."""

import pandas as pd
import pytest

from openclean.pipeline import stream


@pytest.fixture
def ds():
    """Get a data frame stream with three columns ('A', 'B', 'C') and ten rows
    such that values in 'A' are all 'A', in 'B' from 0 .. 9 and in
    'C' from 9 .. 0.
    """
    data = list()
    for i in range(10):
        data.append(['A', i, 9 - i])
    df = pd.DataFrame(data=data, columns=['A', 'B', 'C'])
    return stream(df)
