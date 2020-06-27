# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) matches a
given data type constraint.
"""

from openclean.function.base import All, One
from openclean.function.eval.base import Eval
from openclean.function.value.datatype import (
    is_datetime, is_float, is_int, is_nan, to_int, to_float
)


# -- Type checker predicates --------------------------------------------------

class IsDatetime(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row are of type date or can be converted to a date. For
    value lists the for all flag determines whether all values have to be dates
    or at least one.
    """
    def __init__(self, columns, formats=None, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are dates.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        typecast: bool, default=True
            Attempt to parse string values as dates if True.
        formats: string or list(string)
            Date format string using Python strptime() format directives. This
            can be a list of date formats.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_datetime(value, formats=formats, typecast=typecast)

        if for_all:
            func = All(predicate=func)
        else:
            func = One(predicate=func)
        super(IsDatetime, self).__init__(
            func=func,
            columns=columns,
            is_unary=False
        )


class IsInt(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row are of type integer or can be converted to an
    integer. For value lists the for all flag determines whether all values
    have to be integer or at least one.
    """
    def __init__(self, columns, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are integer.
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        typecast: bool, default=True
            Cast string values to integer if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_int(value, typecast=typecast)

        if for_all:
            func = All(predicate=func)
        else:
            func = One(predicate=func)
        super(IsInt, self).__init__(
            func=func,
            columns=columns,
            is_unary=False
        )


class IsFloat(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row are of type float or can be converted to a float
    value. For value lists the for all flag determines whether all values have
    to be floats or at least one.
    """
    def __init__(self, columns, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are floats.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        typecast: bool, default=True
            Cast string values to float if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_float(value, typecast=typecast)

        if for_all:
            func = All(predicate=func)
        else:
            func = One(predicate=func)
        super(IsFloat, self).__init__(
            func=func,
            columns=columns,
            is_unary=False
        )


class IsNaN(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row are of the special type NaN (not a number). For value
    lists the for_all flag determines whether all values have to be NaN or at
    least one.
    """
    def __init__(self, columns, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are of type NaN.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = is_nan
        if for_all:
            func = All(predicate=func)
        else:
            func = One(predicate=func)
        super(IsNaN, self).__init__(
            func=func,
            columns=columns,
            is_unary=False
        )


# -- Type converters ----------------------------------------------------------

class Float(Eval):
    """Convert a given value to float. Raises an error if the given value
    cannot be converted to float and the raise error flag is True. If the
    flag is False, a given default value will be returned for thoses values
    that cannot be converted to float.
    """
    def __init__(self, columns, default_value=None, raise_error=False):
        """Create an instance of an float type cast function.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        default_value: scalar, default=None
            Default value that is being returned for values that cannot be cast
            to float if the raise_error flag is False.
        raise_error: bool, default=False
            Raise ValueError if the value cannot be cast to float.
        """

        def cast(value):
            return to_float(
                value,
                default_value=default_value,
                raise_error=raise_error
            )

        super(Float, self).__init__(
            func=cast,
            columns=columns,
            is_unary=True
        )


class Int(Eval):
    """Convert a given value to integer. Raises an error if the given value
    cannot be converted to integer and the raise error flag is True. If the
    flag is False, a given default value will be returned for thoses values
    that cannot be converted to integer.
    """
    def __init__(self, columns, default_value=None, raise_error=False):
        """Create an instance of an integer type cast function.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        default_value: scalar, default=None
            Default value that is being returned for values that cannot be cast
            to integer if the raise_error flag is False.
        raise_error: bool, default=False
            Raise ValueError if the value cannot be cast to integer.
        """

        def cast(value):
            return to_int(
                value,
                default_value=default_value,
                raise_error=raise_error
            )

        super(Int, self).__init__(
            func=cast,
            columns=columns,
            is_unary=True
        )
