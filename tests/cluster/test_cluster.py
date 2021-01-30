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
    assert c['a'] == 4
    assert c['b'] == 3
    assert c['c'] == 2


def test_cluster_from_counter():
    """Test adding to a cluster with frequency count values."""
    c = Cluster().add('a', count=5).add('b', count=10).add('a', count=6)
    assert c['a'] == 11
    assert c['b'] == 10


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


def test_cluster_to_mapping():
    """Test converting a cluster to a mapping dictionary."""
    c = Cluster().add('a', count=5).add('b', count=10)
    assert c.to_mapping() == {'a': 'b'}
    assert c.to_mapping(target='c') == {'a': 'c', 'b': 'c'}
