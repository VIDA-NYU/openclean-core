# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string helper functions."""

import pytest

from openclean.function.text import to_len, to_lower, to_title, to_upper


@pytest.mark.parametrize(
    'value,func,result',
    [
        ('abc', to_len, 3),
        (42, to_len, 2),
        ('ABC', to_lower, 'abc'),
        (42, to_lower, '42'),
        ('abc', to_title, 'Abc'),
        (42, to_title, '42'),
        ('abc', to_upper, 'ABC'),
        (42, to_upper, '42')
    ]
)
def test_string_function(value, func, result):
    """Test string helper functions on different values."""
    assert func(value) == result
