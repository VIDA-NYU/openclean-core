# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for aggregation functions."""

import pytest

from openclean.function.value.aggregate import Longest, Max, Min, Shortest


def test_aggr_longest():
    """Test aggregator that returns a constant value function for the longest
    value in a given list.
    """
    f = Longest().prepare(['B', 'AB', 'ABC'])
    assert f(0) == 'ABC'


def test_aggr_min_max():
    """Test max aggregator that returns a constant value function for the
    maximum value in a given list.
    """
    f = Max().prepare(['A', 'B', 'C'])
    assert f(0) == 'C'


def test_aggr_min():
    """Test min aggregator that returns a constant value function for the
    minimum value in a given list.
    """
    f = Min().prepare(['A', 'B', 'C'])
    assert f(0) == 'A'


def test_aggr_shortest():
    """Test aggregator that returns a constant value function for the shortest
    value in a given list.
    """
    f = Shortest().prepare(['B', 'AB', 'ABC'])
    assert f(0) == 'B'


def test_aggr_shortest_with_tiebreaker():
    """Test aggregator that returns a constant value function for the shortest
    value in a given list. Use a tiebreaker function to break the tie between
    two shortest values.
    """
    # Error if no tiebreaker is given.
    with pytest.raises(ValueError):
        Shortest().prepare(['A', 'B', 'AB', 'ABC'])
    # Solve tie breaker using Min.
    f = Shortest(tiebreaker=Min()).prepare(['A', 'B', 'AB', 'ABC'])
    assert f(0) == 'A'
    # Solve tie breaker using Min.
    f = Shortest(tiebreaker=Max()).prepare(['A', 'B', 'AB', 'ABC'])
    assert f(0) == 'B'
