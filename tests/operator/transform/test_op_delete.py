# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for delete operator."""

from openclean.function.column import Col
from openclean.function.predicate.comp import Gt
from openclean.operator.transform.delete import delete


def test_ignore_operator(employees):
    """Use comparison between values in two columns to ignore rows."""
    # There are two rows with empty values in attribute Age
    d1 = delete(employees, predicate=Gt('Age', Col('Salary', as_type=float)))
    assert d1.shape == (6, 3)
    # Fank is not in the list
    for name in d1['Name']:
        assert name != 'Frank'
