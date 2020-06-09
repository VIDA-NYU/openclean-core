# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Simple operator that returns a dictionary that contains a mapping of
original values in a data frame column(s) to results of applying a given value
function on them.
"""

from openclean.data.sequence import Sequence
from openclean.function.base import CallableWrapper, ValueFunction


# -- Functions ----------------------------------------------------------------

def mapping(df, columns, func):
    """Get the mapping of values that are modified by a given value function.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: callable or openclean.function.base.ValueFunction
        Callable or value function that accepts a single value as the argument.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    if not isinstance(func, ValueFunction):
        func = CallableWrapper(func=func)
    values = Sequence(df=df, columns=columns)
    result = dict()
    if not func.is_prepared():
        func = func.prepare(values)
    for value in values:
        if value not in result:
            result[value] = func.eval(value)
    return result
