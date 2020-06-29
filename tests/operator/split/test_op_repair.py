# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the repair operator."""

import pytest

from openclean.data.column import Column
from openclean.function.value.mapping import Lookup
from openclean.operator.split.repair import repair


def test_repair_operator(nyc311):
    """Test repair operator using mapping function that raises errors for
    unknown values.
    """
    # Complete mapping does not raise errors
    mapping = {
        'BRONX': 'BX',
        'BROOKLYN': 'BK',
        'MANHATTAN': 'MN',
        'QUEENS': 'QN',
        'STATEN ISLAND': 'SI'
    }
    f = Lookup(mapping, raise_error=True)
    df_succ, df_fail = repair(nyc311, 'borough', f)
    assert df_succ.shape == nyc311.shape
    assert df_fail.shape == (0, len(nyc311.columns))
    # Incomplete mapping will raise error for missing STATEN ISLAND (20 rows).
    mapping = {
        'BRONX': 'BX',
        'BROOKLYN': 'BK',
        'MANHATTAN': 'MN',
        'QUEENS': 'QN'
    }
    f = Lookup(mapping, raise_error=True)
    df_succ, df_fail = repair(nyc311, 'borough', f)
    assert df_succ.shape == (nyc311.shape[0] - 20, len(nyc311.columns))
    assert df_fail.shape == (20, len(nyc311.columns))
    # Ensure that the columns are instances of the Column class
    for col in df_succ.columns:
        assert isinstance(col, Column)
    for col in df_fail.columns:
        assert isinstance(col, Column)
    # Key error is raised if not caught.
    with pytest.raises(KeyError):
        repair(nyc311, 'borough', f, exceptions=ValueError)
    repair(nyc311, 'borough', f, exceptions=KeyError)
    assert df_succ.shape == (nyc311.shape[0] - 20, len(nyc311.columns))
    assert df_fail.shape == (20, len(nyc311.columns))
