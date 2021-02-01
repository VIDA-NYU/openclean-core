# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the key collision cluster method."""

from collections import Counter

import pytest

from openclean.cluster.key import key_collision


def key_generator(value):
    """Simple key generator that maps all values to lower case."""
    return value.lower()


def test_key_collision_cluster_with_empty_input():
    """Test collision key clustering with empty input."""
    clusters = key_collision(values=[], func=key_generator)
    assert len(clusters) == 0


def test_collision_key():
    """Test accessing the collision key for generated clusters."""
    clusters = key_collision(
        values=['a', 'A', 'b', 'B', 'C'],
        func=key_generator,
        minsize=2
    )
    assert {c.key for c in clusters} == {'a', 'b'}


@pytest.mark.parametrize(
    'minsize,values,result',
    [
        (2, ['a', 'b', 'c', 'A'], {'a'}),
        (2, ['A', 'b', 'c', 'a'], {'A'}),
        (2, Counter(['a', 'b', 'c', 'A', 'A', 'A']), {'A'}),
        (1, ['a', 'b', 'c', 'A'], {'a', 'b', 'c'})
    ]
)
def test_key_collision_cluster(minsize, values, result):
    """Test the key collision cluster method (for single thread and multiple
    threads).
    """
    for threads in [1, 2]:
        clusters = key_collision(
            values=values,
            func=key_generator,
            minsize=minsize,
            threads=threads
        )
        assert {c.suggestion() for c in clusters} == result
