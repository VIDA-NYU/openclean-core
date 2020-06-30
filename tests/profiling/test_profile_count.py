# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

from openclean.function.eval.aggregate import Avg
from openclean.function.eval.base import Gt
from openclean.function.eval.datatype import Float, Int
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


def test_count_with_unprepared_statement(schools):
    """Test counting over a list of values that are generated by an evaluation
    function that needs to be prepared before being evaluated.
    """
    col = Float('min_class_size', default_value=0)
    assert count(schools, Gt(col, Avg(col))) == 54


def test_single_column_counter(schools):
    """Test counter over single column expressions."""
    # Use a prepared evaluation function as value generator.
    int_count = count(schools, 'grade', is_int)
    assert int_count == 30
    no_int_count = count(schools, 'grade', is_int, truth_value=False)
    assert no_int_count == 70
    int_count = count(schools, Int('grade', default_value=0), is_int)
    assert int_count == 100
