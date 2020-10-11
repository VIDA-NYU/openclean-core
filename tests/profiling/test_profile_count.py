# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

import pytest

from openclean.function.eval.datatype import Int
from openclean.function.value.datatype import is_int
from openclean.profiling.count import count


def test_multi_column_counter(agencies):
    """Test counting for a multi-column expression."""
    bk_ny = count(
        agencies,
        columns=['borough', 'state'],
        truth_value=['BK', 'NY']
    )
    assert bk_ny == 6


def test_single_column_counter(schools):
    """Test counter over single column expressions."""
    # Use a prepared evaluation function as value generator.
    int_count = count(schools, 'grade', is_int)
    assert int_count == 30
    no_int_count = count(schools, 'grade', is_int, truth_value=False)
    assert no_int_count == 70
    # -- Error cases ----------------------------------------------------------
    # Using evaluation functions as value generators is (no longer) supported.
    with pytest.raises(ValueError):
        count(schools, Int('grade', default_value=0), is_int)
