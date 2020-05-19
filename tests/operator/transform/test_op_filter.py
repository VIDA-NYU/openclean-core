# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for filter operator."""

from openclean.function.column import Col
from openclean.function.predicate.comp import Gt
from openclean.function.predicate.string import Tokens
from openclean.operator.transform.filter import filter


def test_filter_operator(employees, nyc311):
    """Use comparison between values in two columns to filter rows."""
    # There are two rows with empty values in attribute Age
    d1 = filter(employees, predicate=Gt('Age', Col('Salary', as_type=float)))
    assert d1.shape == (1, 3)
    assert d1.iloc[0]['Name'] == 'Frank'
    # Filter all rows in NYC 311 that have a descriptor with there tokens
    # separated by a comma
    d1 = filter(nyc311, predicate=Tokens('descriptor', ',', 3))
    assert d1.shape == (38, 4)
