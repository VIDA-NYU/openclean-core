# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the default string matcher and vocabulary matcher
implementations.
"""

import pytest

from openclean.function.matching.base import (
    DefaultVocabularyMatcher, ExactMatch
)
from openclean.function.matching.tests import DummyStringMatcher


VOCABULARY = [
    'Tokyo',
    'Paris',
    'Berlin',
    'Sydney',
    'Shanghai'
    'New York',
    'Abu Dhabi',
    'Edinburgh',
    'Rio de Janeiro',
]


@pytest.mark.parametrize('use_cache', [True, False])
def test_default_vocabulary_matcher_caching(use_cache):
    """Ensure that the default matcher show the same behavior independently
    of using result caching or not.
    """
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        cache_results=use_cache
    )
    matches = vocab.find_matches('Chicago')
    assert len(matches) == 2
    assert set(vocab.matched_values('Chicago')) == {'Berlin', 'Sydney'}


@pytest.mark.parametrize(
    'query,best_matches,threshold_matches',
    [('Chicago', 2, 6), ('Rio de Janeiro', 1, 4)]
)
def test_default_vocabulary_matcher_config(
    query, best_matches, threshold_matches
):
    """Test correct behavior of different configuration settings (all results,
    bast results, result threshold) for the default vocabulary matcher.
    """
    # All matches
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        best_matches_only=False,
        no_match_threshold=0.
    )
    matches = vocab.find_matches(query)
    assert len(matches) == len(VOCABULARY)
    # Best matches only
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        best_matches_only=True,
        no_match_threshold=0.
    )
    matches = vocab.find_matches(query)
    assert len(matches) == best_matches
    # Matches greater than 0.5
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        best_matches_only=False,
        no_match_threshold=0.5
    )
    matches = vocab.find_matches(query)
    assert len(matches) == threshold_matches


def test_exact_string_matcher():
    """Test the exact string matcher implementation."""
    # The default setting is case-sensitive
    f = ExactMatch()
    assert f.score('ABC', 'abc') == 0
    assert f.score('XYZ', 'XYZ') == 1
    assert f.score('ABC', 0) == 0
    # Test case-insensitive matching
    f = ExactMatch(ignore_case=True)
    assert f.score('ABC', 'abc') == 1
    assert f.score('XYZ', 'XYZ') == 1
    assert f.score('ABC', 0) == 0
