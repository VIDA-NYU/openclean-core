# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the mapping class that represent lookup tables for value
normalization.
"""

from collections import Counter

import pytest

from openclean.data.mapping import Mapping, ExactMatch, NoMatch, StringMatch


def test_add_to_mapping():
    """Test adding matching results to a mapping."""
    mapping = Mapping()\
        .add('A', [StringMatch(term='B', score=0.5)])\
        .add('A', [ExactMatch(term='C')])\
        .add('A', [NoMatch(term='D')])\
        .add('B', [ExactMatch(term='C'), ExactMatch(term='D')])\
        .add('B', [NoMatch(term='E')])
    assert len(mapping) == 2
    assert len(mapping['A']) == 3
    assert len(mapping['B']) == 3
    assert [(m.term, m.score) for m in mapping['A']] == [('B', 0.5), ('C', 1.0), ('D', 0.0)]
    assert [(m.term, m.score) for m in mapping['B']] == [('C', 1.0), ('D', 1.0), ('E', 0.0)]


def test_filter_mapping():
    """Test the match counter for mappings."""
    mapping = Mapping()\
        .add('A', [ExactMatch(term='B'), ExactMatch(term='C')])\
        .add('B', [ExactMatch(term='C')])\
        .add('C', [ExactMatch(term='B'), NoMatch(term='D'), NoMatch(term='E')])\
        .filter(['A', 'D'])
    assert len(mapping) == 2
    assert mapping['A'] == [ExactMatch(term='B'), ExactMatch(term='C')]
    assert mapping['D'] == []


def test_init_mapping():
    """Test initializing a mappings object."""
    # Initialize from a 1:1 string mapping.
    mapping = Mapping({'A': 'B', 'B': 'C'})
    assert len(mapping) == 2
    assert mapping['A'] == [ExactMatch('B')]
    assert mapping['B'] == [ExactMatch('C')]
    # Initialize from existing mapping.
    mapping.add('C', [NoMatch('E')])
    mapping = Mapping(mapping)
    assert len(mapping) == 3
    assert mapping['A'] == [ExactMatch('B')]
    assert mapping['B'] == [ExactMatch('C')]
    assert mapping['C'] == [NoMatch('E')]


def test_mapping_to_lookup():
    """Test converting a mapping to a lookup dictionary."""
    mapping = Mapping()\
        .add('A', [ExactMatch(term='B'), ExactMatch(term='C')])\
        .add('B', [ExactMatch(term='C')])\
        .add('C', [])
    assert mapping.to_lookup() == {'B': 'C'}
    assert Mapping().add('B', [ExactMatch(term='C')]).to_lookup() == {'B': 'C'}
    # Errors when raise error flag is True and the mapping contains elements with
    # zero or more than one elements.
    with pytest.raises(RuntimeError):
        Mapping()\
            .add('A', [ExactMatch(term='B'), ExactMatch(term='C')])\
            .add('B', [ExactMatch(term='C')])\
            .to_lookup(raise_error=True)
    with pytest.raises(RuntimeError):
        Mapping()\
            .add('B', [ExactMatch(term='C')])\
            .add('C', [])\
            .to_lookup(raise_error=True)


def test_match_counts():
    """Test the match counter for mappings."""
    mapping = Mapping()
    assert mapping.match_counts() == Counter()
    mapping = mapping\
        .add('A', [ExactMatch(term='B'), ExactMatch(term='C')])\
        .add('B', [ExactMatch(term='C')])\
        .add('C', [ExactMatch(term='B'), NoMatch(term='D'), NoMatch(term='E')])
    assert mapping.match_counts() == Counter({'A': 2, 'B': 1, 'C': 3})


def test_matched_and_unmatched():
    """Test functions that return matched and unmatched term lists."""
    mapping = Mapping()\
        .add('A', [ExactMatch(term='B'), ExactMatch(term='C')])\
        .add('B', [NoMatch(term='C')])\
        .add('C', [])
    # All matched terms ('A' and 'B')
    all_matched = mapping.matched()
    assert set(all_matched.keys()) == set({'A', 'B'})
    assert len(all_matched['A']) == 2
    assert len(all_matched['B']) == 1
    # Terms with single matches onlye ('B')
    single_match = mapping.matched(single_match_only=True)
    assert set(single_match.keys()) == set({'B'})
    assert len(single_match['B']) == 1
    # Terms witout matches.
    assert mapping.unmatched() == set({'C'})


def test_update_mapping():
    """Test updating a mapping."""
    mapping = Mapping()\
        .add('A', [NoMatch(term='B'), NoMatch(term='B')])\
        .add('B', [StringMatch(term='C', score=0.5)])\
        .add('C', [])\
        .update({'A': 'D', 'B': 'C'})
    assert len(mapping) == 3
    assert len(mapping['A']) == 1
    assert mapping['A'][0] == ExactMatch(term='D')
    assert len(mapping['B']) == 1
    assert mapping['B'][0] == ExactMatch(term='C')
    assert mapping.unmatched() == set({'C'})
    # Update previously empty mapping.
    mapping = mapping.update({'C': 'A'})
    assert len(mapping) == 3
    assert len(mapping['C']) == 1
    assert mapping['C'][0] == ExactMatch(term='A')
    # Error for unknown keys
    with pytest.raises(KeyError):
        mapping.update({'A': 'D', 'E': 'A'})
