# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for embedding generators."""

from openclean.profiling.embedding.base import embedding
from openclean.profiling.embedding.feature import StandardFeatures


def test_standard_feature_embedding(nyc311):
    """Test generating feature vectors for column values using the default
    feature vector generator for scalar values.
    """
    vec = embedding(nyc311, 'street', StandardFeatures())
    assert vec.data.shape == (258, 7)
