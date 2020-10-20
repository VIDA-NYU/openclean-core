# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the datatype converter."""

import pytest

from datetime import datetime
from openclean.profiling.datatype.convert import DefaultConverter


@pytest.mark.parametrize(
    'input_value,result_label,result_value',
    [
        (1, 'int', 1),
        ('2', 'int', 2),
        (2.3, 'float', 2.3),
        ('3.4', 'float', 3.4),
        (datetime(2020, 5, 15), 'date', datetime(2020, 5, 15)),
        ('2020-5-15', 'date', datetime(2020, 5, 15)),
        ('ABC', 'str', 'ABC'),
        ([1, 2, 3], 'unknown', [1, 2, 3])
    ]
)
def test_convert_scalar_value(input_value, result_label, result_value):
    """Test converting the data type of different input values."""
    value, label = DefaultConverter().convert(input_value)
    assert value == result_value
    assert label == result_label
