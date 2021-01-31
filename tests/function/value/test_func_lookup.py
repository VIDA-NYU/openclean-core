# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the lookup functions."""

import pytest

from openclean.function.value.mapping import Lookup
from openclean.util.core import scalar_pass_through


def test_lookup_table():
    """Simple test of the lookup table function."""
    # Default behavior
    f = Lookup({'A': 1, 'B': 2, 'C': 3})
    assert f('A') == 1
    assert f('B') == 2
    assert f('D') is None
    f = Lookup({'A': 1, 'B': 2, 'C': 3}, default=-1)
    assert f('A') == 1
    assert f('D') == -1
    # Return self as default
    f = Lookup({'A': 1, 'B': 2, 'C': 3}, default=scalar_pass_through)
    assert f('A') == 1
    assert f('D') == 'D'
    # Error for missing keys
    f = Lookup({'A': 1, 'B': 2, 'C': 3}, default=-1, raise_error=True)
    assert f('A') == 1
    with pytest.raises(KeyError):
        f('D')
    # Convert values to string
    f = Lookup({'1': 'one', '2': 'two'})
    assert f(1) is None
    f = Lookup({'1': 'one', '2': 'two'}, as_string=True)
    assert f(1) == 'one'
