# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that operate on string values."""

from openclean.function.base import Apply, EvalFunction
from openclean.function.value.string import Capitalize as capitalize
from openclean.function.value.string import Lower as lower
from openclean.function.value.string import Upper as upper


class Capitalize(object):
    """String function that capitalizes the first letter in argument values."""
    def __new__(cls, columns, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        return Apply(
            columns=columns,
            func=capitalize(as_string=as_string, raise_error=raise_error)
        )


class Format(EvalFunction):
    """Eval function that returns a formated string based on a given format
    template and a variable list of input values.
    row.
    """
    def __init__(self, template, values):
        """Initialize the format template and the value generation function.

        Parameters
        ----------
        template: string
            String format template.
        values: callable
            Function that is used to get the (list of) values that are used
            as input for the format template.

        Raises
        ------
        ValueError
        """
        self.template = template
        self.func = values

    def eval(self, values):
        """Extract values for the format template from the given data frame
        row. Use the str.format method to get the formated string for the
        extracted values.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        val = self.func(values)
        # Need to distinguish between value generators that return a single
        # value or a variable value list.
        if type(val) in [list, tuple]:
            return self.template.format(*val)
        else:
            return self.template.format(val)

    def prepare(self, df):
        """If the value generator function is a prepared callable the
        respective prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        # Prepare the value generator if it is a prepared function.
        if isinstance(self.func, EvalFunction):
            self.func.prepare(df)
        return self


class Lower(object):
    """String function that converts argument values to lower case."""
    def __new__(cls, columns, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        return Apply(
            columns=columns,
            func=lower(as_string=as_string, raise_error=raise_error)
        )


class Upper(object):
    """String function that converts argument values to upper case."""
    def __new__(cls, columns, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        return Apply(
            columns=columns,
            func=upper(as_string=as_string, raise_error=raise_error)
        )
