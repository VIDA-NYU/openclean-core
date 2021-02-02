# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the cluster index class."""

from openclean.cluster.base import Cluster
from openclean.cluster.index import ClusterIndex


def test_add_to_index():
    """Test adding clusters to the index."""
    clusters = ClusterIndex()
    assert clusters.add(Cluster().add('A', 1))
    assert not clusters.add(Cluster().add('A', 1))
    assert clusters.add(Cluster().add('A', 1).add('B', 1))
    assert clusters.add(Cluster().add('B', 1).add('B', 1))
    assert not clusters.add(Cluster().add('B', 1).add('A', 1))
    assert not clusters.add(Cluster().add('A', 1).add('B', 1))
    assert not clusters.add(Cluster().add('B', 1).add('B', 1))
    assert not clusters.add(Cluster().add('B', 1).add('A', 1))
