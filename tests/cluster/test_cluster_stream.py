# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the cluster stream class."""

from openclean.cluster.key import KeyCollision
from openclean.function.value.key.fingerprint import Fingerprint


def test_cluster_multi_value_stream():
    """Test stream cluster functionality using the key collision cluster
    algorithm on a stream of multi-column rows.
    """
    clusterer = KeyCollision(func=Fingerprint()).open(['col'])
    for val in ['A B', 'B C', 'B   c', 'C\tb']:
        clusterer.consume(0, val.split())
    clusters = clusterer.close()
    assert len(clusters) == 1
    assert len(clusters[0]) == 3


def test_cluster_single_value_stream():
    """Test stream cluster functionality using the key collision cluster
    algorithm on a stream of single-column rows.
    """
    clusterer = KeyCollision(func=Fingerprint()).open(['col'])
    for val in ['A B', 'B C', 'B   c', 'C\tb']:
        clusterer.consume(0, [val])
    clusters = clusterer.close()
    assert len(clusters) == 1
    assert len(clusters[0]) == 3
