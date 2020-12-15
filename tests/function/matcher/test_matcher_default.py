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

from openclean.data.mapping import ExactMatch, NoMatch, StringMatch
from openclean.function.matching.base import DefaultStringMatcher, ExactSimilarity, best_matches
from openclean.function.matching.tests import DummyMatcher


def test_best_matches_function():
    """Test the bast matches mapping generator function."""
    matcher = DefaultStringMatcher(
        vocabulary=['A', 'B', 'C'],
        similarity=DummyMatcher([ExactMatch('A')])
    )
    # Ignore vocabulary entries (default).
    mapping = best_matches(['A', 'D', 'E'], matcher)
    assert len(mapping) == 2
    assert mapping['D'] == [ExactMatch('A')]
    assert mapping['E'] == [ExactMatch('A')]
    # Include vocabulary entries.
    mapping = best_matches(['A', 'D', 'E'], matcher, include_vocab=True)
    assert len(mapping) == 3
    assert mapping['A'] == [ExactMatch('A')]
    assert mapping['D'] == [ExactMatch('A')]
    assert mapping['E'] == [ExactMatch('A')]


@pytest.mark.parametrize('use_cache', [True, False])
def test_default_vocabulary_matcher_caching(use_cache):
    """Ensure correct behavior for result caching in the default matcher."""
    matcher = DummyMatcher([ExactMatch('A')])
    vocab = DefaultStringMatcher(
        vocabulary=[],
        similarity=matcher,
        cache_results=use_cache
    )
    assert vocab.find_matches('B') == [ExactMatch('A')]
    # Update the result set for the test matcher
    matcher.result = [ExactMatch('B')]
    # The result now depends on whether caching is used or not.
    if use_cache:
        assert vocab.find_matches('B') == [ExactMatch('A')]
    else:
        assert vocab.find_matches('B') == [ExactMatch('B')]


@pytest.mark.parametrize(
    'matches,best_matches,threshold_matches',
    [
        ([], 0, 0),
        ([ExactMatch('A')], 1, 1),
        ([ExactMatch('A'), ExactMatch('B'), StringMatch(term='C', score=0.1)], 2, 2),
        ([ExactMatch('A'), StringMatch(term='B', score=0.6), StringMatch(term='C', score=0.1)], 1, 2),
        ([StringMatch(term='C', score=0.1), StringMatch(term='B', score=0.4), ExactMatch('A')], 1, 1),
        ([ExactMatch('A'), StringMatch(term='B', score=0.4), StringMatch(term='C', score=0.1)], 1, 1)
    ]
)
def test_default_vocabulary_matcher_config(matches, best_matches, threshold_matches):
    """Test correct behavior of different configuration settings (all results,
    bast results, result threshold) for the default vocabulary matcher.
    """
    # All matches
    vocab = DefaultStringMatcher(
        vocabulary=[],
        similarity=DummyMatcher(matches),
        best_matches_only=False,
        no_match_threshold=0.
    )
    assert len(vocab.find_matches('TEST')) == len(matches)
    # Best matches only
    vocab = DefaultStringMatcher(
        vocabulary=[],
        similarity=DummyMatcher(matches),
        best_matches_only=True,
        no_match_threshold=0.
    )
    assert len(vocab.find_matches('TEST')) == best_matches
    # Matches greater than 0.5
    vocab = DefaultStringMatcher(
        vocabulary=[],
        similarity=DummyMatcher(matches),
        best_matches_only=False,
        no_match_threshold=0.5
    )
    assert len(vocab.find_matches('TEST')) == threshold_matches


def test_exact_string_matcher():
    """Test the exact string matcher implementation."""
    # The default setting is case-sensitive
    f = ExactSimilarity()
    assert f.score(['ABC'], 'abc') == [NoMatch('ABC')]
    assert f.score(['XYZ'], 'XYZ') == [ExactMatch('XYZ')]
    assert f.score(['ABC'], 0) == [NoMatch('ABC')]
    # Test case-insensitive matching
    f = ExactSimilarity(ignore_case=True)
    assert f.score(['ABC'], 'abc') == [ExactMatch('ABC')]
    assert f.score(['XYZ'], 'XYZ') == [ExactMatch('XYZ')]
    assert f.score(['ABC'], 0) == [NoMatch('ABC')]
