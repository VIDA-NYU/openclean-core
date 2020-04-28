# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string length feature function."""

import pytest

from openclean.function.feature.length import Length, NormalizedLength


def test_length_feature():
    """Simple test for the value length feature function."""
    # Default settings
    f = Length()
    assert f('abc') == 3
    assert f(1234) == 4
    assert f('') == 0
    # Default for non-strings
    f = Length(as_string=False, default_value=-1)
    assert f('abc') == 3
    assert f(1234) == -1
    assert f('') == 0
    # Raise error for non-strings
    f = Length(as_string=False, default_value=-1, raise_error=True)
    assert f('abc') == 3
    with pytest.raises(ValueError):
        f(1234)
    assert f('') == 0


def test_normalized_length_feature():
    """Test the normalized value length feature."""
    values = [1, 'AB', 'ABC', 1000]
    f = NormalizedLength().get_function(data=values)
    feature = [f(v) for v in values]
    assert feature == [0, 1/3, 2/3, 1.0]
