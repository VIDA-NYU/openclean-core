# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for domain inclusion operators."""

from openclean.function.value.domain import is_in, is_not_in


def test_func_domain():
    """Simple tests for domain inclusion and exclusion."""
    # -- IsIn -----------------------------------------------------------------
    f = is_in(['A', 'B', 'C'])
    assert f('A')
    assert not f('D')
    assert not f(('A', 'B'))
    # List of tuples
    f = is_in([('A', 'B'), ('C',)])
    assert not f('A')
    assert not f('C')
    assert f(('A', 'B'))
    # -- IsNotIn --------------------------------------------------------------
    f = is_not_in(['A', 'B', 'C'])
    assert not f('A')
    assert f('D')
    assert f(('A', 'B'))
    # List of tuples
    f = is_not_in([('A', 'B'), ('C',)])
    assert f('A')
    assert f('C')
    assert not f(('A', 'B'))
