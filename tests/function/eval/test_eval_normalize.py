# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for normalization functions."""

import pandas as pd
import pytest

from openclean.function.eval.datatype import Int
from openclean.function.eval.normalize import (
    DivideByTotal, MaxAbsScale, MinMaxScale
)


@pytest.fixture
def dataset():
    return pd.DataFrame(
        data=[
            ['Alice', '23'],
            ['Bob', 34],
            ['Claire', 42],
            ['Dave', 'UNKNOWN']
        ],
        columns=['Name', 'Age']
    )


@pytest.mark.parametrize(
    'op,results',
    [
        (DivideByTotal, [23/100, 34/100, 42/100, 1/100]),
        (MaxAbsScale, [23/42, 34/42, 1, 1/42]),
        (MinMaxScale, [22/41, 33/41, 1, 0])
    ]
)
def test_normalization_function_with_int(dataset, op, results):
    f = op(Int('Age', default_value=1)).prepare(dataset)
    assert [f.eval(row) for _, row in dataset.iterrows()] == results


@pytest.mark.parametrize(
    'op,results',
    [
        (DivideByTotal, [23/99, 34/99, 42/99, 0]),
        (MaxAbsScale, [23/42, 34/42, 1, 0]),
        (MinMaxScale, [0, (34-23)/(42-23), 1, 0])
    ]
)
def test_normalization_woth_ignore(dataset, op, results):
    f = op(Int('Age'), raise_error=False, default_value=0).prepare(dataset)
    assert [f.eval(row) for _, row in dataset.iterrows()] == results
