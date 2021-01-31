# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the default string matcher and vocabulary matcher
implementations.
"""

import pytest

from openclean.data.mapping import StringMatch
from openclean.function.matching.fuzzy import FuzzySimilarity
from openclean.function.matching.base import DefaultStringMatcher

VOCABULARY = [
    'Tokyo',
    'Paris',
    'Berlin',
    'Sydney',
    'Shanghai',
    'New York',
    'Abu Dhabi',
    'Edinburgh',
    'Rio de Janeiro',
]


@pytest.mark.parametrize(
    'query,best_matches,threshold_matches',
    [('New Shangi', 2, 0), ('Rio de Janero', 1, 1)]
)
def test_default_vocabulary_matcher_config(
    query, best_matches, threshold_matches
):
    """Test correct behavior of different configuration settings (all results,
    bast results, result threshold) for the default vocabulary matcher.
    """
    # All matches
    vocab = DefaultStringMatcher(
        vocabulary=VOCABULARY,
        similarity=FuzzySimilarity(),
        best_matches_only=False,
        no_match_threshold=0.
    )
    matches = vocab.find_matches(query)
    assert len(matches) == best_matches
    for m in matches:
        assert isinstance(m, StringMatch)
    # Best matches only
    vocab = DefaultStringMatcher(
        vocabulary=VOCABULARY,
        similarity=FuzzySimilarity(),
        best_matches_only=True,
        no_match_threshold=0.
    )
    matches = vocab.find_matches(query)
    assert len(matches) == best_matches
    # Matches greater than 0.5
    vocab = DefaultStringMatcher(
        vocabulary=VOCABULARY,
        similarity=FuzzySimilarity(),
        best_matches_only=False,
        no_match_threshold=0.5
    )
    matches = vocab.find_matches(query)
    assert len(matches) == threshold_matches
