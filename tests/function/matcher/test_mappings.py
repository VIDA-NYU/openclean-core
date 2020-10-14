# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the mapping class that represent lookup tables for value
normalization.
"""

import pytest

from openclean.function.matching.base import DefaultVocabularyMatcher
from openclean.function.matching.mapping import Mapping, best_matches
from openclean.function.matching.tests import DummyStringMatcher


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


def test_add_to_mapping():
    """Test adding matching results to a mapping."""
    m = Mapping(values=['A', 'B'])
    # Add additional results for term 'A'
    map_a = m.add('A', [('B', 0.5), ('C', 0.25)])
    assert map_a == [('A', 1.), ('B', 0.5), ('C', 0.25)]
    # Add results for term 'C'
    m.add('C', [('A', 0.7)])
    map_c = m.add('C', [('B', 0.5), ('D', 0.4)])
    assert map_c == [('A', 0.7), ('B', 0.5), ('D', 0.4)]


def test_best_matches():
    """Test generating a mpping of best matches against a given vocabulary."""
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher()
    )
    map = best_matches(
        values=['Chicago', 'London', 'Edinburgh'],
        matcher=vocab
    )
    assert len(map) == 2
    assert map['Chicago'] != []
    assert map['London'] != []
    assert map['Edinburgh'] == []
    map = best_matches(
        values=['Chicago', 'London', 'Edinburgh'],
        matcher=vocab,
        include_vocab=True
    )
    assert len(map) == 3
    assert map['Chicago'] != []
    assert map['London'] != []
    assert map['Edinburgh'] != []


def test_init_mapping():
    """Test initializing objects for the Mapping class."""
    # An empty mapping.
    m = Mapping()
    assert len(m) == 0
    m['A'].append('B')
    assert len(m) == 1
    assert m['A'] == ['B']
    # Mapping with pre-initialized vocabulary.
    m = Mapping(values=['A', 'B'])
    assert len(m) == 2
    assert m['A'] == [('A', 1.)]
    assert m['B'] == [('B', 1.)]
    assert m['C'] == []
    m['C'].append('A')
    assert m['C'] == ['A']

def test_match_counts():
    """Test generating the count of matches against a given vocabulary."""
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        no_match_threshold=0.5
    )
    map = Mapping()
    values = ['Chicago', 'London', 'Edinburgh','Universal Studios Florida (c)']
    for val in values:
        map.add(val, vocab.find_matches(val))

    counts = map.match_counts()
    assert len(counts) == len(map)
    for key in counts:
        assert len(map[key]) == counts[key]


def test_matched_and_unmatched():
    """Test matched and unmatched methods."""
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        no_match_threshold=0.5
    )
    map = Mapping()
    values = ['Chicago', 'London', 'Edinburgh', 'Universal Studios Florida (c)']
    for val in values:
        map.add(val, vocab.find_matches(val))

    matched = map.matched()
    assert len(matched) == 3
    for m in map['Chicago']:
        assert m[0] in ['Shanghai', 'New York']

    unmatched = map.unmatched()
    assert 'Universal Studios Florida (c)' in unmatched

    # test single match only
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        no_match_threshold=0
    )
    map = Mapping()
    for val in values:
        map.add(val, vocab.find_matches(val))

    matched = map.matched(single_match_only=True)
    assert len(matched) == 1
    assert map['Universal Studios Florida (c)'][0][0] == 'Rio de Janeiro'

def test_filter_mapping():
    """Test matched and unmatched methods."""
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        no_match_threshold=0.5
    )
    map = Mapping()
    values = ['Chicago', 'London', 'Edinburgh', 'Universal Studios Florida (c)']
    for val in values:
        map.add(val, vocab.find_matches(val))

    filtered = map.filter(['Chicago', 'Universal Studios Florida (c)'])
    assert len(filtered) == 2
    for m in filtered['Chicago']:
        assert m[0] in ['Shanghai', 'New York']
    assert filtered['Universal Studios Florida (c)'] == []

def test_update_mapping():
    """Test replace and error case"""
    vocab = DefaultVocabularyMatcher(
        vocabulary=VOCABULARY,
        matcher=DummyStringMatcher(),
        no_match_threshold=0
    )
    map = Mapping()
    values = ['Chicago', 'London', 'Edinburgh', 'Universal Studios Florida (c)']
    for val in values:
        map.add(val, vocab.find_matches(val))

    assert map['Universal Studios Florida (c)'][0][0] == 'Rio de Janeiro'
    replacements = {'Universal Studios Florida (c)': 'Florida'}
    map.update(updates=replacements)
    assert map['Universal Studios Florida (c)'] == [('Florida', 1.)]

    # Missing Key
    with pytest.raises(KeyError):
        map.update({'unknown':'unknown'})
