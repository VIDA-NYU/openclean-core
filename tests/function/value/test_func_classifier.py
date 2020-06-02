# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the classifier callbacl function."""

import math
import pytest

from datetime import datetime

from openclean.function.value.classifier import ClassLabel, ValueClassifier
from openclean.function.value.datatype import Datetime, Float, Int, is_int


def test_datatype_classification():
    """Test data type classification of scalar values."""
    values = [datetime.now().isoformat(), 1, '1', 1.2, '1.2', math.nan, 'abc']
    f = ValueClassifier(Datetime(label='date'), Int(), Float())
    labels = f.apply(values)
    assert labels == ['date', 'int', 'int', 'float', 'float', 'float', None]
    # Without type casting.
    f = ValueClassifier(
        Datetime(typecast=False),
        Int(typecast=False),
        Float(typecast=False),
        default_label='str'
    )
    labels = f.apply(values)
    assert labels == ['str', 'int', 'str', 'float', 'str', 'float', 'str']
    # Raise errors for unknown types.
    f = ValueClassifier(Int(), Float(), default_label='str', raise_error=True)
    with pytest.raises(ValueError):
        f.apply(values)


def test_classify_with_different_truth_value():
    """Test changing the truth value and none labels."""
    f = ClassLabel(
        predicate=is_int,
        label='no-int',
        truth_value=False,
        none_label='int'
    )
    classifier = ValueClassifier(f, none_label='int', default_label='is-int')
    labels = classifier.apply([1, '1', 'ABC'])
    assert labels == ['is-int', 'is-int', 'no-int']
