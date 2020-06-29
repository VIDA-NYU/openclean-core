# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for mapping evaluation function."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col
from openclean.function.eval.mapping import Map, Replace
from openclean.function.value.base import scalar_pass_through as pass_through
from openclean.function.value.mapping import Lookup


def test_dataframe_lookup(agencies):
    """Test lookup for multiple columns using a data frame as dictionary."""
    df = pd.DataFrame(data=[
        ['BK', 'NY', 'Brooklyn', 'New York'],
        ['BX', 'NY', 'Bronx', 'New York']
    ])
    # Replace unknown values with None.
    f = Map((Col('borough'), Col('state')), df).prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {('Bronx', 'New York'), ('Brooklyn', 'New York'), None}
    # Use pass-through for unknown value.
    f = Map((Col('borough'), Col('state')), df, default_value=pass_through)
    f = f.prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {
        ('Bronx', 'New York'),
        ('Brooklyn', 'New York'),
        ('MN', 'NY'),
        ('BX', 'NJ')
    }
    # Raise error for unknown values.
    f = Map((Col('borough'), Col('state')), df, raise_error=True)
    f = f.prepare(agencies)
    with pytest.raises(KeyError):
        [f.eval(row) for _, row in agencies.iterrows()]


def test_dictionary_lookup(agencies):
    """Test map single column using a dictionary."""
    mapping = {'BK': 'Brooklyn', 'BX': 'Bronx'}
    # Replace unknown values with None.
    f = Map('borough', mapping).prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {'Bronx', 'Brooklyn', None}
    # We get the same result with a default lookup value function.
    f = Map('borough', Lookup(mapping)).prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {'Bronx', 'Brooklyn', None}
    # Use pass-through for unknown value.
    f = Map('borough', mapping, default_value=pass_through).prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {'Bronx', 'Brooklyn', 'MN'}
    # Raise error for unknown values.
    f = Map('borough', mapping, raise_error=True).prepare(agencies)
    with pytest.raises(KeyError):
        [f.eval(row) for _, row in agencies.iterrows()]


def test_replace_function(agencies):
    """Test conditional replace (if-then-else) function."""
    lookup = Lookup({'BK': 'Brooklyn', 'BX': 'Bronx'})
    # Use default else statement.
    f = Replace('borough', lookup, lookup).prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {'Bronx', 'Brooklyn', 'MN'}
    # Custom else-statement.
    f = Replace('borough', lookup, lookup, elsestmt=str.lower)
    f = f.prepare(agencies)
    boroughs = set([f.eval(row) for _, row in agencies.iterrows()])
    assert boroughs == {'Bronx', 'Brooklyn', 'mn'}
