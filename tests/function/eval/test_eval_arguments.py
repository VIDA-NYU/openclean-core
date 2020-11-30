# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the evaluation function decorator that passes static
arguments on to the evaluation function.
"""

import pandas as pd
import pytest

from openclean.function.eval.base import Eval


# -- Test data ----------------------------------------------------------------

@pytest.fixture
def dataset():
    return pd.DataFrame(
        data=[['x,1:2', 0], ['y,2:3', 1], ['z,4:5', 2]],
        columns=['A', 'B']
    )


# -- Test functions -----------------------------------------------------------


def my_extract(value, delim=',', index=0):
    """Split a given string and return value at given index position."""
    return value.split(delim)[index]


def my_extract_concat(text, number, conc, delim=',', index=0):
    """Split a given string and return value at given index position."""
    value = my_extract(text, delim=delim, index=index)
    return '{}{}{}'.format(value, conc, number)


# -- Unit tests ---------------------------------------------------------------

@pytest.mark.parametrize(
    'args,results',
    [
        ({}, ['x', 'y', 'z']),
        ({'delim': ':'}, ['x,1', 'y,2', 'z,4']),
        ({'delim': ':', 'index': 1}, ['2', '3', '5'])
    ]
)
def test_run_unary_eval_with_arguments(args, results, dataset):
    """Run a wrapped unary evaluation function with different sets of
    arguments.
    """
    values = Eval(columns='A', func=my_extract, args=args).eval(dataset)
    assert values == results


@pytest.mark.parametrize(
    'args,results',
    [
        ({'conc': ':'}, ['x:0', 'y:1', 'z:2']),
        ({'conc': '-', 'delim': ':'}, ['x,1-0', 'y,2-1', 'z,4-2']),
        ({'conc': '-', 'delim': ':', 'index': 1}, ['2-0', '3-1', '5-2'])
    ]
)
def test_run_tenary_eval_with_arguments(args, results, dataset):
    """Run a wrapped tenary evaluation function with different sets of
    arguments.
    """
    f = Eval(columns=['A', 'B'], func=my_extract_concat, args=args)
    values = f.eval(dataset)
    assert values == results
