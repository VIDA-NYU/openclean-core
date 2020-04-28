# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that operate on string values."""

from openclean.function.base import Apply, EvalFunction, SingleColumnEval

import openclean.function.value.string as sfunc


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
            func=sfunc.capitalize(as_string=as_string, raise_error=raise_error)
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

    def exec(self, values):
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
            func=sfunc.lower(as_string=as_string, raise_error=raise_error)
        )


class Split(SingleColumnEval):
    """Split value in a data frame column based on a given separator. Allows to
    validate the number of returned tokens and raise a ValueError if the number
    of tokens does not match the expected number of tokens.
    """
    def __init__(
        self, columns, sep=None, validate=None, as_string=False,
        raise_error=False
    ):
        """Initialize the the source column and the separation delimiter.
        Raises a ValueError if more than one column is specified.

        Parameters
        ----------
        columns: int or string
            Single column index or name.
        sep: string, optional
            Delimiter string.
        validate: int or callable, optional
            Validate the number of generated tokens against the given count or
            predicate. Raises ValueError if the validation fails.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.

        Raises
        ------
        ValueError
        """
        # Raise an error if more than one column is specified.
        if not type(columns) in [int, str]:
            raise ValueError('invalid column {}'.format(columns))
        super(Split, self).__init__(
            columns=columns,
            func=sfunc.split(
                sep=sep,
                validate=validate,
                as_string=as_string,
                raise_error=raise_error
            )
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
            func=sfunc.upper(as_string=as_string, raise_error=raise_error)
        )
