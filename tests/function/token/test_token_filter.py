# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for basic token list filters."""

from openclean.function.token.filter import FirstLastFilter, MinMaxFilter, TokenFilter
from openclean.function.value.base import UnpreparedFunction
from openclean.function.value.domain import IsInDomain


TOKENS = ['B', 'A', 'C']


def test_first_last_filter():
    """Test the first/last token filter."""
    assert FirstLastFilter().transform(TOKENS) == ['B', 'C']


def test_min_max_filter():
    """Test the min/max token filter."""
    assert MinMaxFilter().transform(TOKENS) == ['A', 'C']


def test_prepared_token_filter():
    """Test token filter with a prepared value function."""
    f = TokenFilter(predicate=IsInDomain(domain={'A', 'D'}))
    assert f.transform(TOKENS) == ['A']


def test_unprepared_token_filter():
    """Test token filter with a value function that needs to be prepared."""

    class TestFunc(UnpreparedFunction):
        """Filter values base on the maximum of a given list."""
        _max = None

        def __call__(self, value):
            return value == self._max

        def prepare(self, values):
            self._max = max(values)
            return self

    f = TokenFilter(predicate=TestFunc())
    assert f.transform(TOKENS) == ['C']
