# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from openclean.function.base import Eval, EvalFunction
from openclean.function.column import Col

import openclean.function.value.comp as vfunc


# -- Generic compare operator -------------------------------------------------

class ExpressionComparator(EvalFunction):
    """Generic comparator for comparing column values against a value
    expression. Since scalar compare operators are wrappers around a constant
    value, a new scalar compare operator has to be instantiated for each data
    frame row after the value expresssion has been evaluated.
    """
    def __init__(self, lhs_columns, rhs_expression, factory):
        """Initialize the column(s) (lhs) whose values are compared against the
        given value expression (rhs). For both arguments a evaluation function
        is expected.

        Parameters
        ----------
        lhs_columns: openclean.function.base.EvalFunction
            Single column or list of column index positions or column names.
        rhs_expression: openclean.function.base.EvalFunction
            Value expression agains which column values are compared
        factory: callable
            Factory function to create an instance of the comparator for the
            result of evaluating the value expression.
        """
        self.lhs_columns = lhs_columns
        self.rhs_expression = rhs_expression
        self.factory = factory

    def prepare(self, df):
        """Prepare both evaluation functions (lhs and rhs).

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        self.lhs_columns.prepare(df)
        self.rhs_expression.prepare(df)
        return self

    def exec(self, values):
        """Evaluate the compare expression on the given data frame row.
        Evaluates the value expression first to get a (scalar) value. That
        value is then used to create an instance of the compare operator using
        the factory function.

        Parameters
        ----------
        values: pandas.core.series.Series
            Pandas data frame row object

        Returns
        -------
        bool
        """
        # Evaluate the right hand side function to get the 'constant' value
        # against which the values in the left hand side columns are compared.
        # The value is used to create an instance of the comparator.
        comp = self.factory(self.rhs_expression(values))
        # Evaluate the generated compare operator on the value returned by the
        # left hand side of the operator.
        return comp(self.lhs_columns(values))


# -- Implementations for the standard value comparators -----------------------

class Eq(object):
    """Simple comparator for single columns values that tests for equality with
    a given value (expression).
    """
    def __new__(cls, columns, value, ignore_case=False, raise_error=False):
        """Create an instance of a single column equality predicate. The type
        of the returned object depends on whether the given value is a constant
        or a callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        ignore_case: bool, optional
            Ignore case in comparison if set to True.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar equality comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.eq(
                    value=val,
                    ignore_case=ignore_case,
                    raise_error=raise_error
                )
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            eq = vfunc.eq(
                value=value,
                ignore_case=ignore_case,
                raise_error=raise_error
            )
            return Eval(func=eq, columns=columns)


class EqIgnoreCase(object):
    """Shortcut for comparing single column values in a case-insenstive manner.
    """
    def __new__(cls, columns, value, raise_error=False):
        """Create an instance of the equality comparator with the ignore case
        flag set to True.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        return Eq(
            columns=columns,
            value=value,
            ignore_case=True,
            raise_error=raise_error
        )


class Geq(object):
    """Simple comparator for single column values that tests if a column value
    is greater or equal than a given constant value or a value expression.
    """
    def __new__(cls, columns, value, raise_error=False):
        """Create an instance of the greater than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.geq(value=val, raise_error=raise_error)
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            geq = vfunc.geq(value=value, raise_error=raise_error)
            return Eval(func=geq, columns=columns)


class Gt(object):
    """Simple comparator for single column values that tests if a column value
    is greater than a given constant value or a value expression.
    """
    def __new__(cls, columns, value, raise_error=False):
        """Create an instance of the greater than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.gt(value=val, raise_error=raise_error)
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            gt = vfunc.gt(value=value, raise_error=raise_error)
            return Eval(func=gt, columns=columns)


class Leq(object):
    """Simple comparator for single column values that tests if a column value
    is less or equal than a given constant value or a value expression.
    """
    def __new__(cls, columns, value, raise_error=False):
        """Create an instance of the less or equal comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.leq(value=val, raise_error=raise_error)
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            leq = vfunc.leq(value=value, raise_error=raise_error)
            return Eval(func=leq, columns=columns)


class Lt(object):
    """Simple comparator for single column values that tests if a column value
    is less than a given constant value or a value expression.
    """
    def __new__(cls, columns, value, raise_error=False):
        """Create an instance of the less than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.lt(value=val, raise_error=raise_error)
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            lt = vfunc.lt(value=value, raise_error=raise_error)
            return Eval(func=lt, columns=columns)


class Neq(object):
    """Simple comparator for single column values that tests if a column value
    is not equal to a given constant value or a value expression.
    """
    def __new__(cls, columns, value, ignore_case=False, raise_error=False):
        """Create an instance of the not equal comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: scalar or openclean.function.base.EvalFunction
            Value expression agains which column values are compared.
        ignore_case: bool, optional
            Ignore case in comparison if set to True.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if isinstance(value, EvalFunction):
            # Define a factory for scalar comparators. The factory is
            # called with the result of the evaluated expression.
            def _comp_factory(val):
                return vfunc.neq(
                    value=val,
                    ignore_case=ignore_case,
                    raise_error=raise_error
                )
            return ExpressionComparator(
                lhs_columns=Col(columns),
                rhs_expression=value,
                factory=_comp_factory
            )
        else:
            neq = vfunc.neq(
                value=value,
                ignore_case=ignore_case,
                raise_error=raise_error
            )
            return Eval(func=neq, columns=columns)
