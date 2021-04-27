# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for mapping functions."""

import pandas as pd
import pytest

from openclean.function.eval.mapping import Lookup, Standardize


@pytest.fixture
def dataset():
    """Simple dataset with the name and age of four people."""
    return pd.DataFrame(
        data=[
            ['Alice', 'R&D'],
            ['Bob', 'Accnt'],
            ['Claudia', 'R&D'],
            ['Rob', 'Service']
        ],
        columns=['Name', 'Department']
    )


def test_lookup_eval(dataset):
    """Test the lookup evaluation function."""
    table = {'R&D': 'Research', 'Accnt': 'Accounting'}
    func = Lookup(columns='Department', mapping=table)
    assert func.eval(dataset) == ['Research', 'Accounting', 'Research', 'Service']


def test_standardize_eval(dataset):
    """Test the standardize evaluation function."""
    mapping = {'R&D': 'Research', 'Accnt': 'Accounting'}
    func = Standardize(columns='Department', mapping=mapping)
    assert func.eval(dataset) == ['Research', 'Accounting', 'Research', 'Service']
