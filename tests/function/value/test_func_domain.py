# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for domain inclusion operators."""

import pytest

from openclean.data.mapping import ExactMatch
from openclean.function.matching.base import DefaultStringMatcher
from openclean.function.matching.tests import DummyMatcher
from openclean.function.value.domain import (
    BestMatch, IsInDomain, IsNotInDomain
)


def test_func_best_match():
    """Test finding best matches in a controlled vocabulary."""
    domain = ['abc', 'ab', 'ac', 'b']
    f = BestMatch(
        matcher=DefaultStringMatcher(
            vocabulary=domain,
            similarity=DummyMatcher([ExactMatch('b')]),
            no_match_threshold=0.5
        )
    )
    assert f.eval('b') == 'b'
    assert f.eval('A') == 'b'
    # -- Error cases ----------------------------------------------------------
    # Empty result set.
    f = BestMatch(
        matcher=DefaultStringMatcher(
            vocabulary=domain,
            similarity=DummyMatcher([]),
            no_match_threshold=0.5
        )
    )
    with pytest.raises(ValueError):
        f.eval('ABCDEFGHIJKLMNOP')
    # Multiple best matches
    f = BestMatch(
        matcher=DefaultStringMatcher(
            vocabulary=domain,
            similarity=DummyMatcher([ExactMatch('A'), ExactMatch('B')]),
            no_match_threshold=0.5
        )
    )
    with pytest.raises(ValueError):
        f.eval('ad')


def test_func_domain():
    """Simple tests for domain inclusion and exclusion."""
    # -- IsInDomain -----------------------------------------------------------
    f = IsInDomain(['A', 'B', 'C'])
    assert f('A')
    assert not f('a')
    assert not f('D')
    assert not f(('A', 'B'))
    f = IsInDomain(['A', 'B', 'C'], ignore_case=True)
    assert f('A')
    assert f('a')
    assert not f('D')
    assert not f(('A', 'B'))
    # List of tuples
    f = IsInDomain([('A', 'B'), ('C',)])
    assert not f('A')
    assert not f(('a', 'B'))
    assert not f('C')
    assert f(('A', 'B'))
    f = IsInDomain([('A', 'B'), ('C',)], ignore_case=True)
    assert not f('A')
    assert f(('a', 'B'))
    assert not f('C')
    assert f(('A', 'B'))
    # -- IsNotInDomain --------------------------------------------------------
    f = IsNotInDomain(['A', 'B', 'C'])
    assert not f('A')
    assert f('D')
    assert f(('A', 'B'))
    # List of tuples
    f = IsNotInDomain([('A', 'B'), ('C',)])
    assert f('A')
    assert f('C')
    assert not f(('A', 'B'))
    assert f(('A', 'b'))
    f = IsNotInDomain([('A', 'B'), ('C',)], ignore_case=True)
    assert f('A')
    assert f('C')
    assert not f(('A', 'B'))
    assert not f(('A', 'b'))
