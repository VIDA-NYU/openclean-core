# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for profiling operators that compute cardinalities for scalar
predicates on a given data frame.
"""

import os
import pytest

from openclean.data.load import dataset
from openclean.profiling import counts, DistinctValues
from openclean.function.eval.predicate.scalar.comp import Eq, Gt


DIR = os.path.dirname(os.path.realpath(__file__))
CSV_FILE = os.path.join(DIR, '../data/dataset_with_missing_values.csv')


def test_cardinality_profiler():
    """Compute counts for an input data frame."""
    ds = dataset(CSV_FILE)
    # The default counts includes the number of empty cells, the number of
    # non-empty cells, and the number of distinct values.
    d1 = counts(df=ds)
    assert d1.loc['is_empty', 'Name'] == 0
    assert d1.loc['is_empty', 'Age'] == 2
    assert d1.loc['is_empty', 'Salary'] == 0
    assert d1.loc['is_not_empty', 'Name'] == 7
    assert d1.loc['is_not_empty', 'Age'] == 5
    assert d1.loc['is_not_empty', 'Salary'] == 7
    assert d1.loc['distinct', 'Name'] == 7
    assert d1.loc['distinct', 'Age'] == 6
    assert d1.loc['distinct', 'Salary'] == 7
    # Use a custom list of predicates.
    predicates = [Eq('Alice'), Gt(30, typecast=True)]
    index = ['eq_alice', 'gt_30']
    d1 = counts(df=ds, predicates=predicates, index=index)
    assert d1.loc['eq_alice', 'Name'] == 1
    assert d1.loc['eq_alice', 'Age'] == 0
    assert d1.loc['eq_alice', 'Salary'] == 0
    assert d1.loc['gt_30', 'Name'] == 0
    assert d1.loc['gt_30', 'Age'] == 4
    assert d1.loc['gt_30', 'Salary'] == 0


def test_distinct_value_set():
    """Count distinct values for all columns. Test accessing the resulting sets
    of values for each column.
    """
    ds = dataset(CSV_FILE)
    values = DistinctValues()
    counts(df=ds, predicates=[values])
    # -- Value set ------------------------------------------------------------
    names = values.get_values('Name')
    assert len(names) == 7
    for n in ['Alice', 'Bob', 'Claudia', 'Dave', 'Eileen', 'Frank', 'Gertrud']:
        assert n in names
    # -- Data frame -----------------------------------------------------------
    df = values.to_df('Name')
    assert len(df.columns) == 2
    assert 'Name' in df.columns
    assert 'count' in df.columns
    assert df.size == 14
    df = values.to_df('Name', include_count=False, value_column='Person')
    assert len(df.columns) == 1
    assert 'Person' in df.columns
    assert df.size == 7
    df = values.to_df('Name', include_count=False, sort_order='desc')
    names = list(df['Name'])
    assert names[0] == 'Gertrud'
    assert names[-1] == 'Alice'
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        values.get_values(10)
    with pytest.raises(ValueError):
        values.get_values('Persons')
