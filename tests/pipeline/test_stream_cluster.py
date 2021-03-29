# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct operator in data processing pipelines."""

from openclean.cluster.key import KeyCollision
from openclean.function.value.key.fingerprint import Fingerprint


def test_cluster_rows(ds):
    """Test distinct count over a stream of rows."""
    assert len(ds.cluster(KeyCollision(Fingerprint()))) == 5
