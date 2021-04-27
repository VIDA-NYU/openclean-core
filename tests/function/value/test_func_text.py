# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string helper functions."""

import pytest

from openclean.function.value.text import AlphaNumeric, to_len, to_lower, to_title, to_upper


@pytest.mark.parametrize(
    'value,result',
    [
        ('ABC', True),
        ('ab12', True),
        ('1-3', False),
        ('-1', False),
        ('123', True),
        (123, True),
        (12.34, False),
        (' ', False),
        ('a#1', False),
        ('!@#$', False)
    ]
)
def test_alphanumeric_predicate(value, result):
    """Test the alphanimeric character predicate."""
    assert AlphaNumeric().eval(value) == result


@pytest.mark.parametrize(
    'value,func,result',
    [
        ('abc', to_len, 3),
        (42, to_len, 2),
        ('ABC', to_lower, 'abc'),
        (42, to_lower, 42),
        ('abc', to_title, 'Abc'),
        (42, to_title, 42),
        ('abc', to_upper, 'ABC'),
        (42, to_upper, 42)
    ]
)
def test_string_function(value, func, result):
    """Test string helper functions on different values."""
    assert func(value) == result
