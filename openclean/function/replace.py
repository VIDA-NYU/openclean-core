# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value mapping and replacement operators."""

import pandas as pd

from openclean.data.transform import to_lookup
from openclean.function.base import (
    Eval, EvalFunction, FullRowEval, is_var_func
)
from openclean.function.constant import Const
from openclean.function.value.domain import is_in
from openclean.function.value.replace import lookup, replace, varreplace


# -- Value mapping ------------------------------------------------------------

class Lookup(object):
    """Evaluation function that maps single values (scalar or tuple) to single
    values (scalar or tuple) based on a given lookup mapping.
    """
    def __new__(cls, columns, mapping, raise_error=False):
        """Initialize the constant return value. Define the behavior for
        optional type conversion.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        mapping: dicy or openclean.function.value.replace.Lookup
            Mapping from input to output values. The type of the keys in the
            mapping dictionary are expected to match the value type that is
            defined by the columns list.
        raise_error: bool, optional
            Controls the behavior of a lookup table that is created if the
            mapping argument is a dictionary. If the flag is True the default
            behavior of the created lookup for unknonwn keys is to raise an
            error. Otherwise, the input value is returned as output.
        """
        if isinstance(mapping, pd.DataFrame):
            # If the mapping is a data frame create a mapping from it. The
            # number of columns has to be even since the data frame will
            # be split in the missdle between source and target columns.
            if len(mapping.columns) % 2 != 0:
                raise ValueError('cannot convert data frame to mapping')
            mid = len(mapping.columns) // 2
            key_columns = list(range(mid))
            target_columns = list(range(mid, len(mapping.columns)))
            mapping = to_lookup(mapping, key_columns, target_columns)
        if isinstance(mapping, dict):
            # If the mapping is a dictionary wrap it in a lookup table.
            if raise_error:
                mapping = lookup(mapping, for_missing='error')
            else:
                mapping = lookup(mapping, for_missing='self')

        # Wrap the lookup function into a variable lookup if input values come
        # from more than one column.
        if is_var_func(columns):
            mapping = VarLookup(mapping)
        return Eval(func=mapping, columns=columns)


class VarLookup(object):
    """Wrap a lookup table in a callable that accespts a variable list of
    argument values. Calls the wrapped lookup with a single value (tuple)
    containing all arguments.
    """
    def __init__(self, mapping):
        """Initialize the lookup table.

        Parameters
        ----------
        mapping: openclean.function.value.replace.Lookup
            Lookup table.
        """
        self.mapping = mapping

    def __call__(self, *args):
        """Call wrapped mapping with single tuple containing all argument
        values.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        list
        """
        return self.mapping(args)


# -- Conditional replace ------------------------------------------------------

class IfThenReplace(EvalFunction):
    def __init__(self, cond, values, pass_through=None):
        """"Initialize the update condition and the modification function.

        Parameters
        ----------
        cond: callable or openclean.function.base.EvalFunction
            Function that is evaluated to identify rows that are modified.
        values: callable or openclean.function.base.EvalFunction
            Function that is used to compute the modified values for rows that
            satisfy the replacement condition.
        pass_through: openclean.function.base.EvalFunction, optional
            Function that is used to compute the unmodified result for rows
            that do not satisfy the modification condition.
        """
        # Ensure that all functions are evaluation functions
        if not isinstance(cond, EvalFunction):
            cond = FullRowEval(cond)
        if not isinstance(values, EvalFunction):
            if callable(values):
                values = FullRowEval(values)
            else:
                values = Const(values)
        if pass_through is not None:
            if not isinstance(pass_through, EvalFunction):
                pass_through = FullRowEval(pass_through)
        self.cond = cond
        self.values = values
        self.pass_through = pass_through

    def eval(self, values):
        """Evaluate the function on the given data frame row.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        if self.cond(values):
            return self.values(values)
        else:
            return self.pass_through(values)

    def prepare(self, df):
        """Prepare the associated evaluation functions.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        self.cond.prepare(df)
        self.values.prepare(df)
        if self.pass_through is not None:
            self.pass_through.prepare(df)
        return self


class Replace(object):
    """Evaluation function that replaces values in columns of data frame rows
    if the row values match a given condition.
    """
    def __new__(cls, columns, cond, values=None):
        """"Initialize the list of modified columns, the update condition, and
        the modification function.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        cond: callable or openclean.function.base.EvalFunction
            Function that is evaluated to identify rows that are modified.
        values: callable or openclean.function.base.EvalFunction
            Function that is used to compute the modified values for rows that
            satisfy the replacement condition.
        """
        if isinstance(cond, dict) and values is None:
            values = lookup(cond, for_missing='self')
            cond = is_in(cond)
        # Wrap the replace function into a variable replace if input values
        # come from more than one column.
        if is_var_func(columns):
            func = varreplace(cond=cond, values=values)
        else:
            func = replace(cond=cond, values=values)
        return Eval(func=func, columns=columns)


class replace(object):
    """Replace function for single argument. Returns pre-defined replacement
    value for input values that satisfy a given condition.
    """
    def __init__(self, cond, values):
        """Initialize the replacement condition and the replacement value.

        Parameters
        ----------
        cond: callable or class
            Function that is evaluated to identify values that are modified.
        values: constant or callable or class
            Constant return value for modified values or function that is used
            to compute the modified value.
        """
        # Ensure that the condition is callable.
        if isinstance(cond, type):
            cond = cond()
        elif not callable(cond):
            cond = eq(cond)
        self.cond = cond
        # If values is a class object create an instance of that class.
        if isinstance(values, type):
            values = values()
        self.values = values

    def __call__(self, value):
        """Return a modified value if the given argument satisfies the
        replacement condition. Otherwise, the value is returned as is.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        list
        """
        if self.cond(value):
            return self.values(value) if callable(self.values) else self.values
        return value


class varreplace(object):
    """Replace function for variable argument lists. Returns pre-defined
    replacement value for input values that satisfy a given condition.
    """
    def __init__(self, cond, values):
        """Initialize the replacement condition and the replacement values.

        Parameters
        ----------
        cond: callable or class
            Function that is evaluated to identify values that are modified.
        values: constant or callable or class
            Constant return value for modified values or function that is used
            to compute the modified value.
        """
        # Ensure that the condition is callable.
        if isinstance(cond, type):
            cond = cond()
        elif not callable(cond):
            cond = eq(cond)
        self.cond = cond
        # If values is a clas object create an instance of that class.
        if isinstance(values, type):
            values = values()
        self.values = values

    def __call__(self, *args):
        """Return a modified value if the given argument satisfies the
        replacement condition. Otherwise, the value is returned as is.

        Parameters
        ----------
        args: lists
            Variable list of argument values.

        Returns
        -------
        list
        """
        if self.cond(*args):
            return self.values(*args) if callable(self.values) else self.values
        return args
