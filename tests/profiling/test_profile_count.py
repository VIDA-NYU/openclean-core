# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

from openclean.function.value.datatype import is_int
from openclean.profiling.count import count, Count, counts, Counts


def test_profile_single_counter(schools):
    """Test single predicate counter function."""
    int_count = count(schools, columns='grade', predicate=is_int)
    assert int_count == 30
    no_int_count = count(
        schools,
        columns='grade',
        predicate=is_int,
        truth_value=False
    )
    assert no_int_count == 70
    counter = Count(predicate=is_int)
    assert counter.name() == 'is_int'


def test_profile_multi_counters(schools):
    """Test counter for multiple predicates."""

    def start_with_zero(value):
        return value.startswith('0')

    result = counts(
        schools,
        columns='grade',
        predicates=[is_int, start_with_zero]
    )
    assert result == {'is_int': 30, 'start_with_zero': 73}
    counter = Counts(predicates=[is_int, start_with_zero])
    assert counter.name() == 'counts'
    counter = Counts(predicates=[is_int, start_with_zero], name='my-count')
    assert counter.name() == 'my-count'
