# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the classifier callbacl function."""

import math
import pytest

from openclean.function.classifier import ValueClassifier
from openclean.function.value.datatype import is_date, is_int, is_float, is_nan


def test_datatype_classification():
    """Test data type classification of scalar values."""
    f = ValueClassifier(
        classifier=[is_date('%m/%d/%y'), is_int(), is_float(), is_nan()],
        labels=['date', 'int', 'float', 'nan'],
        default_label='str'
    )
    assert f('07/15/20') == 'date'
    assert f(1) == 'int'
    assert f('1') == 'int'
    assert f(1.2) == 'float'
    assert f('1.2') == 'float'
    assert f(math.nan) == 'float'
    assert f('abc') == 'str'
    # Without type cast
    f = ValueClassifier(
        classifier=[
            is_date('%m/%d/%y'),
            is_int(False),
            is_float(False),
            is_nan()
        ],
        labels=['date', 'int', 'float', 'nan'],
        default_label='str'
    )
    assert f('07/15/20') == 'date'
    assert f(1) == 'int'
    assert f('1') == 'str'
    assert f(1.2) == 'float'
    assert f('1.2') == 'str'
    assert f(math.nan) == 'float'
    assert f('abc') == 'str'
    # -- Change truth values
    f = ValueClassifier(
        classifier=[is_float(), is_nan()],
        labels=['float', 'nan'],
        truth_values=[False, True],
        default_label='str'
    )
    assert f(1.2) == 'str'
    assert f(math.nan) == 'nan'
    assert f('abc') == 'float'


def test_classifiaction_errors():
    """Test error cases for classification."""
    # Raise error if no predicate matches
    f = ValueClassifier(
        classifier=[is_int(), is_float()],
        labels=['int', 'float'],
        default_label='str',
        raise_error=True
    )
    assert f(1) == 'int'
    with pytest.raises(ValueError):
        f('abc')
    # Invalid arguments
    with pytest.raises(ValueError):
        ValueClassifier(classifier=[is_int(), is_float()], labels=['int'])
    with pytest.raises(ValueError):
        ValueClassifier(classifier=is_int(), labels='i', truth_values=[1, 2])
