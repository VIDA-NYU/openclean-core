# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the threshold untility functions."""

import pytest

from openclean.util.threshold import ge, gt, le, lt, to_threshold


@pytest.mark.parametrize(
    'func,threshold,results',
    [
        (ge, 0.5, [False, True, True]),
        (gt, 0.5, [False, False, True]),
        (le, 0.5, [True, True, False]),
        (lt, 0.5, [True, False, False]),
        (to_threshold, 0.4, [False, True, True]),
        (to_threshold, 0.5, [False, False, True]),
        (to_threshold, 1, [False, False, True]),
        (to_threshold, lt(0.5), [True, False, False])
    ]
)
def test_threshold_functions(func, threshold, results):
    """Create an instance of a threshold object. The threshold will be
    evaluated on the values 0.5 and 1. Test whether the instantiated threshold
    yields the expected results for those two values.
    """
    f = func(threshold)
    assert [f(x) for x in [0.25, 0.5, 1]] == results
