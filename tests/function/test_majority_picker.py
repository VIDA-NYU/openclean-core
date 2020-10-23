# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for different implementations of the majority picker class."""

from collections import Counter

import pytest

from openclean.function.picker import MostFrequentValue, OnlyOneValue
from openclean.function.value.normalize import divide_by_total

@pytest.mark.parametrize(
    'picker,values,result,raises_error',
    [
        (OnlyOneValue(), Counter({'A': 3}), 'A', False),
        (OnlyOneValue(), Counter(), None, False),
        (OnlyOneValue(), Counter(), None, True),
        (OnlyOneValue(), Counter({'A': 3, 'B': 1}), 'Z', True),
        (MostFrequentValue(), Counter({'A': 3}), 'A', False),
        (MostFrequentValue(), Counter({'A': 3, 'B': 2}), 'A', False),
        (MostFrequentValue(), Counter({'A': 3, 'B': 3}), None, False),
        (MostFrequentValue(), Counter({'A': 3, 'B': 3}), 'C', True),
        (MostFrequentValue(), Counter(), None, False),
        (MostFrequentValue(), Counter(), None, True),
        (MostFrequentValue(threshold=2), Counter({'A': 3}), 'A', False),
        (MostFrequentValue(threshold=2), Counter({'A': 1}), 'B', True),
        (MostFrequentValue(threshold=2), Counter({'A': 3, 'B': 3}), 'C', True),
        (MostFrequentValue(threshold=0.7, normalizer=divide_by_total), Counter({'A': 3, 'B': 1}), 'A', False),
        (MostFrequentValue(threshold=0.7, normalizer=divide_by_total), Counter({'A': 3, 'B': 2}), 'C', True)
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
