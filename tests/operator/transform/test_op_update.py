# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.function.eval.base import Col, Cols, Const
from openclean.function.value.mapping import Lookup
from openclean.operator.stream.collector import Collector
from openclean.operator.transform.update import swap, update, Update

from openclean.function.matching.fuzzy import FuzzySimilarity
from openclean.function.matching.base import DefaultStringMatcher
from openclean.function.matching.base import best_matches
from openclean.function.value.domain import BestMatch


BOROUGHS = [
    'BRONX',
    'BROOKLYN',
    'MANHATTAN',
    'QUEENS',
    'STATEN ISLAND'
]


def test_swap_columns(nyc311):
    """Test swapping values in two columns of a data frame."""
    df = swap(nyc311, 'borough', 'descriptor')
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Result is independent of the order of col1 and col2
    df = swap(nyc311, 'descriptor', 'borough')
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Error for invalid column list
    with pytest.raises(ValueError):
        swap(nyc311, ['descriptor', 'borough'], 'descriptor')


def test_update_single_column(nyc311):
    """Test updating values in a single column of a data frame."""
    mapping = {
        'BRONX': 'BX',
        'BROOKLYN': 'BK',
        'MANHATTAN': 'MN',
        'QUEENS': 'QN',
        'STATEN ISLAND': 'SI'
    }
    df = update(nyc311, 'borough', mapping)
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())
    df = update(nyc311, 'borough', Lookup(mapping))
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())


def test_update_multiple_columns(nyc311):
    """Test updating values in multiple columns of a data frame."""
    df = update(
        df=nyc311,
        columns=['borough', 'descriptor'],
        func=Cols(['descriptor', 'borough'])
    )
    # Ensure that the columns are instances of the Column class
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Error for non-matching (column, value) counts
    with pytest.raises(ValueError):
        update(nyc311, ['borough', 'descriptor'], Col('borough'))


def test_ternary_update_consumer():
    """Test updating a multiple columns in a data stream."""
    collector = Collector()
    func = Const([0, 1])
    consumer = Update(columns=[2, 1], func=func)\
        .open(['A', 'B', 'C'])\
        .set_consumer(collector)
    assert consumer.columns == ['A', 'B', 'C']
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 3
    assert rows[0] == (3, [1, 1, 0])
    assert rows[1] == (2, [4, 1, 0])
    assert rows[2] == (1, [7, 1, 0])


def test_unary_update_consumer():
    """Test updating a single column in a data stream."""
    collector = Collector()
    func = Col(column='A', colidx=0)
    consumer = Update(columns=[1], func=func)\
        .open(['A', 'B', 'C'])\
        .set_consumer(collector)
    assert consumer.columns == ['A', 'B', 'C']
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 3
    assert rows[0] == (3, [1, 1, 3])
    assert rows[1] == (2, [4, 4, 6])
    assert rows[2] == (1, [7, 7, 9])


def test_update_bestmatch_column(nyc311):
    """Test updating values in a single column with their best match mappings
    of a data frame.
    """
    domain_pronounciation = [
        'bronks',
        'brooklen',
        'manhatn',
        'kweenz',
        'staten islen'
    ]

    vocabulary = DefaultStringMatcher(
        vocabulary=domain_pronounciation,
        similarity=FuzzySimilarity(),
        no_match_threshold=0.
    )

    # replace with the best match in the given vocabulary
    df = update(nyc311, 'borough', BestMatch(matcher=vocabulary))
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(domain_pronounciation)

    # Manually create a mapping to let user manipulate it before updating the
    # data frame.
    map = best_matches(values=nyc311['borough'].unique(), matcher=vocabulary)
    # override best match from the vocabulary
    map.update({'STATEN ISLAND': 'statn ayeln'})

    df = update(nyc311, 'borough', map.to_lookup())
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(map.to_lookup().values())
