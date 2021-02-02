# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string similarity constraint function."""

from openclean.function.similarity.base import SimilarityConstraint
from openclean.function.similarity.text import LevenshteinDistance
from openclean.function.value.threshold import GreaterThan


def test_string_similarity_constraint():
    """Test the string similarity constraint function."""
    f = SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.5))
    assert f('BROOKLYN', 'BROKLYN')
    assert not f('BROOKLYN', 'QUEENS')
