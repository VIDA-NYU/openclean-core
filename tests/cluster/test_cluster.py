# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the value cluster class."""

import pytest

from openclean.cluster.base import Cluster


def test_add_cluster_elements():
    """Test adding values to a cluster and iterating over the result."""
    c = Cluster()
    for value in list('acbaabbac'):  # 4xa, 3xb, 2xc
        c.add(value)
    assert len(c) == 3
    assert c[0] == 'a'
    assert c[1] == 'c'
    assert c[2] == 'b'
    with pytest.raises(KeyError):
        c[3]
    counts = {value: count for value, count in c}
    assert counts['a'] == 4
    assert counts['b'] == 3
    assert counts['c'] == 2


def test_cluster_from_counter():
    """Test adding to a cluster with frequency count values."""
    c = Cluster().add('a', count=5).add('b', count=10).add('a', count=6)
    counts = {value: count for value, count in c}
    assert counts['a'] == 11
    assert counts['b'] == 10


@pytest.mark.parametrize(
    'value,suggest',
    [('acbaabbac', 'a'), ('acbaabbbac', 'a'), ('bacbaabbac', 'b')]
)
def test_cluster_suggestion(value, suggest):
    """Test suggesting a common value for values in a cluster."""
    c = Cluster()
    for v in list(value):
        c.add(v)
    assert c.suggestion() == suggest
