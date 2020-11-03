# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for domain predicates for data frame rows."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col, Const
from openclean.function.eval.domain import IsIn, IsNotIn, Lookup


"""Get simple dataset with the name and age of three people."""
dataset =  pd.DataFrame(
        data=[['Alice', 29], ['Bob', 32], ['Claire', 41]],
        columns=['Name', 'Age'],
        index=[65, 23, 98]
    )


"""Set with two names 'Alice' and 'Bob'."""
NAMES = {'Alice', 'Bob', 'Dave'}
PERSONS = {('Claire', 41), ('John', 16)}

@pytest.mark.parametrize(
    'op,columns,domain,result',
    [
        (IsIn, 'Name', NAMES, [True, True, False]),
        (IsNotIn, 'Name', NAMES, [False, False, True]),
        (IsIn, 'Name', dataset['Name'], [True, True, True]),
        (IsNotIn, 'Name', dataset['Name'], [False, False, False]),
        (IsIn, ['Name', 'Age'], PERSONS, [False, False, True]),
        (IsNotIn, ['Name', 'Age'], PERSONS, [True, True, False]),
        (IsIn, ['Name', 'Age'], dataset[['Name', 'Age']], [True, True, True]),
        (IsNotIn, ['Name', 'Age'], dataset[['Name', 'Age']], [False, False, False])
    ]
)
def test_domain_predicate(op, columns, domain, result):
    """Test the domainpredicates for values from a single column."""
    assert op(columns=columns, domain=domain).eval(dataset) == result
    f = op(columns=columns, domain=domain).prepare(dataset.columns)
    assert [f(row) for row in dataset.itertuples(index=False, name=None)] == result


@pytest.mark.parametrize(
    'default,result',
    [
        (None, [('Alicia', 1), ('Bob', 2), ('Claire', 2)]),
        (['Name', 'Age'], [('Alicia', 1), ('Bob', 32), ('Claire', 41)])
    ]
)
def test_multi_column_lookup(default, result):
    """Test using dictionaries as lookup tables using values from a multiple
    columns in the data frame.
    """
    op = Lookup(['Name', Const(2)], {('Alice', 2): ('Alicia', 1)}, default=default)
    assert op.eval(dataset) == result


@pytest.mark.parametrize(
    'default,result',
    [
        (None, ['Alicia', 'Bob', 'Claire']),
        (Const('NoName'), ['Alicia', 'NoName', 'NoName']),
        ('Age', ['Alicia', 32, 41]),
        (Col('Age'), ['Alicia', 32, 41])
    ]
)
def test_single_column_lookup(default, result):
    """Test using dictionaries as lookup tables using values from a single
    column in the data frame.
    """
    op = Lookup('Name', {'Alice': 'Alicia'}, default=default)
    assert op.eval(dataset) == result
