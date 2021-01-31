# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the row counter."""

from openclean.function.eval.datatype import IsInt
from openclean.operator.collector.count import count


def test_count_dataframe_rows(nyc311):
    """Test counting rows and distinct values in in data frames."""
    assert count(nyc311) == 304


def test_single_column_filter(schools):
    """Test counter over single column predicate."""
    # There are 30 rows with grade values that are (or can be cast to) integers
    # and 70 rows with grades that cannot be converted to integers.
    INT_COUNT = 30
    NO_INT_COUNT = 70
    # Count integer values.
    assert count(schools, IsInt('grade')) == INT_COUNT
    # Count non-integers using the same predicate but a custom truth value.
    assert count(schools, IsInt('grade'), truth_value=False) == NO_INT_COUNT
