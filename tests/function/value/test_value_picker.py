# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for different implementations of the majority picker class."""

from collections import Counter

import pytest

from openclean.function.value.normalize import divide_by_total
from openclean.function.value.picker import MajorityVote, OnlyOneValue

@pytest.mark.parametrize(
    'picker,values,result,raises_error',
    [
        (OnlyOneValue(), Counter({'A': 3}), 'A', False),
        (OnlyOneValue(), Counter(), None, False),
        (OnlyOneValue(), Counter(), None, True),
        (OnlyOneValue(), Counter({'A': 3, 'B': 1}), 'Z', True),
        (MajorityVote(), Counter({'A': 3}), 'A', False),
        (MajorityVote(), Counter({'A': 3, 'B': 2}), 'A', False),
        (MajorityVote(), Counter({'A': 3, 'B': 3}), None, False),
        (MajorityVote(), Counter({'A': 3, 'B': 3}), 'C', True),
        (MajorityVote(), Counter(), None, False),
        (MajorityVote(), Counter(), None, True),
        (MajorityVote(threshold=2), Counter({'A': 3}), 'A', False),
        (MajorityVote(threshold=2), Counter({'A': 1}), 'B', True),
        (MajorityVote(threshold=2), Counter({'A': 3, 'B': 3}), 'C', True),
        (MajorityVote(threshold=0.7, normalizer=divide_by_total), Counter({'A': 3, 'B': 1}), 'A', False),
        (MajorityVote(threshold=0.7, normalizer=divide_by_total), Counter({'A': 3, 'B': 2}), 'C', True)
    ]
)
def test_most_frequent_picker(picker, values, result, raises_error):
    """Test whether a given picker selects the expected value from a given
    counter. If the raises_error flag is True the picker is called with the
    raise_error flag True and is expected to raise a ValueError instead of
    returning a value. Otherwise, the picker is only called with the raise_error
    flag set to True if the expected result is not the default value.
    """
    if raises_error:
        with pytest.raises(ValueError):
            picker.pick(values, raise_error=True)
    else:
        assert picker.pick(values, raise_error=result is not None) == result


@pytest.mark.parametrize(
    'picker,values,input,result,raises_error',
    [
        (MajorityVote(), [1, 2, 2], 3, 2, False),
        (MajorityVote(), [0, 1, 2], 3, 3, False),
        (MajorityVote(raise_error=True), [0, 1, 2], 3, 3, True)
    ]
)
def test_value_picker_function(picker, values, input, result, raises_error):
    """Test the functionality of the value picker function for translating
    values in a given input list.
    """
    if raises_error:
        # If the raises_error flag is True then we expect the picker to raise a
        # ValueError during prepare.
        with pytest.raises(ValueError):
            picker.prepare(values)
    else:
        # Execute the prepared picker on the given input and compare to the
        # expected result.
        f = picker.prepare(values)
        assert f(input) == result
