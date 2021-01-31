# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for data type detection and conversion functions."""

import math
import numpy as np
import pytest

from datetime import datetime

from openclean.function.value.datatype import (
    is_datetime, is_float, is_int, is_nan, is_numeric, is_numeric_type,
    to_datetime, to_float, to_int
)


def test_func_is_type():
    """Test type detection predicates."""
    # -- Date -----------------------------------------------------------------
    assert is_datetime(datetime.now().isoformat())
    date = '21/11/06 16:30'
    assert is_datetime(date, formats='%d/%m/%y %H:%M')
    assert not is_datetime(date, formats=['%d/%m/%y'])
    assert is_datetime(date, formats=['%d/%m/%y', '%d/%m/%y %H:%M'])
    # -- Integer --------------------------------------------------------------
    values = [1, 'A', 3.5, [1], np.int64(3), '1', np.float64(4)]
    integers = [is_int(v) for v in values]
    assert integers == [True, False, False, False, True, True, False]
    integers = [is_int(v, typecast=False) for v in values]
    assert integers == [True, False, False, False, True, False, False]
    # -- Float ----------------------------------------------------------------
    floats = [is_float(v) for v in values]
    assert floats == [False, False, True, False, False, True, True]
    floats = [is_float(v, typecast=False) for v in values]
    assert floats == [False, False, True, False, False, False, True]
    # -- Numbers --------------------------------------------------------------
    values = [1, 'A', 3.5, [1], np.int64(3), '1', np.float64(4)]
    numbers = [is_numeric(v) for v in values]
    assert numbers == [True, False, True, False, True, True, True]
    numbers = [is_numeric_type(v) for v in values]
    assert numbers == [True, False, True, False, True, False, True]
    assert not is_numeric('1', typecast=False)
    # -- Nan ------------------------------------------------------------------
    assert not is_nan(1)
    assert not is_nan('A')
    assert is_nan(math.nan)


def test_func_type_cast():
    """Test type cast functions."""
    # -- Date -----------------------------------------------------------------
    assert to_datetime(datetime.now().isoformat()) is not None
    assert to_datetime('ABC') is None
    with pytest.raises(ValueError):
        to_datetime('ABC', raise_error=True)
    # -- Integer --------------------------------------------------------------
    assert to_int(1) == 1
    assert to_int('2') == 2
    assert to_int(3.5) == 3
    assert to_int('3.5') is None
    assert to_int('3.5', default_value=0) == 0
    with pytest.raises(ValueError):
        to_int('3.5', raise_error=True)
    # -- Float ----------------------------------------------------------------
    assert to_float(1) == 1
    assert to_float('2.5') == 2.5
    assert to_float(3.5) == 3.5
    assert to_float('ABC') is None
    assert to_float('ABC', default_value=0) == 0
    with pytest.raises(ValueError):
        to_float('ABC', raise_error=True)
