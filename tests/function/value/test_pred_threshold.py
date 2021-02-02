# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for the threshold predicates."""

import pytest

import openclean.function.value.threshold as th


@pytest.mark.parametrize(
    'cls,results',
    [
        (th.LowerOrEqual, [True, True, False]),
        (th.LowerThan, [True, False, False]),
        (th.GreaterOrEqual, [False, True, True]),
        (th.GreaterThan, [False, False, True])
    ]
)
def test_threshold_predicates(cls, results):
    """Compare values 0.4, 0.5, 0.6 against a threshold of 0.5."""
    f = cls(0.5)
    assert [f(v) for v in [0.4, 0.5, 0.6]] == results
