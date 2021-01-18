# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the key collision cluster method."""

import pytest

from openclean.cluster.key import key_collision


def key_generator(value):
    """Simple key generator that maps all values to lower case."""
    return value.lower()


@pytest.mark.parametrize('minsize,result', [(2, {'a'}), (1, {'a', 'b', 'c'})])
def test_key_collision_cluster(minsize, result):
    """Test the key collision cluster method (for single thread and multiple
    threads).
    """
    values = ['a', 'b', 'c', 'A']
    clusters = key_collision(values=values, func=key_generator, minsize=minsize, threads=1)
    assert {c.suggestion() for c in clusters} == result
    clusters = key_collision(values=values, func=key_generator, minsize=minsize, threads=2)
    assert {c.suggestion() for c in clusters} == result
