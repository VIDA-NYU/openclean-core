# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Evaluate function that returns values from columns in a data frame row."""

from openclean.function.base import Eval, VarApply, is_var_func
from openclean.function.value.datatype import cast
from openclean.function.value.base import scalar_pass_through


class Col(object):
    """Evaluation function that returns value(s) from one or more column(s) in
    the data frame row. Allows to convert values (i.e., change their data type)
    using an optional type converter (callable).
    """
    def __new__(
        self, columns, as_type=None, default_value=None, raise_error=False
    ):
        """Initialize the source column(s). Define the behavior for optional
        type conversion.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        as_type: callable, optional
            Function that converts the data type of a given scalar value.
        default_value: scalar, optional
            Default value that is being returned for values that cannot be
            converted to the specified type if the raise_error flag is False.
        raise_error: bool, optional
            Raise ValueError if the value that is being extracted from a data
            frame row cannot be converted to the specified type.
        """
        # Choose a pass-through function based on the columns argument and the
        # 'as_type' type cast argument.
        if is_var_func(columns):
            # -- Function that accepts a list of arguments
            if as_type is None:
                func = var_pass_through
            else:

                def typecast(value):
                    return cast(
                        value=value,
                        func=as_type,
                        default_value=default_value,
                        raise_error=raise_error
                    )

                func = VarApply(typecast)
        else:
            # -- Function that accepts a single argument
            if as_type is None:
                func = scalar_pass_through
            else:

                def func(value):
                    return cast(
                        value=value,
                        func=as_type,
                        default_value=default_value,
                        raise_error=raise_error
                    )
                    
        return Eval(func=func, columns=columns)


# -- Helper classes and methods -----------------------------------------------

def var_pass_through(*args):
    """Pass-through function for a variable list of argument values.

    Parameters
    ----------
    *args: list
        List of (scalar) values from a data frame row.

    Returns
    -------
    list
    """
    return args
