# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from abc import abstractmethod, ABCMeta

from openclean.function.eval.base import EvalFunction
from openclean.function.eval.constant import Const


# -- Generic compare operator -------------------------------------------------

class ExpressionComparator(EvalFunction, metaclass=ABCMeta):
    """Generic comparator for comparing two column value expressions."""
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Initialize the column(s) (lhs) whose values are compared against the
        given value expression (rhs). For both arguments a evaluation function
        is expected.

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        self.lhs_expression = to_eval(lhs_expression)
        self.rhs_expression = to_eval(rhs_expression)
        self.raise_error = raise_error

    @abstractmethod
    def comp(self, left_values, right_values):
        """Implementation-specific compare function for two values.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    def eval(self, values):
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
        # Evaluate the left hand side and right hand side function to get the
        # values that are passed to the compare function.
        # Call compare method of the implementing subclass. If a TypeError
        # occurs due to incompatible data types the result is False unless the
        # raise type error flag is True.
        try:
            return self.comp(
                left_values=self.lhs_expression(values),
                right_values=self.rhs_expression(values)
            )
        except TypeError as ex:
            if self.raise_error:
                raise ex
            else:
                return False
        except AttributeError:
            return False

    def prepare(self, df):
        """Prepare both evaluation functions (lhs and rhs).

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        self.lhs_expression.prepare(df)
        self.rhs_expression.prepare(df)
        return self


# -- Implementations for the standard value comparators -----------------------

class Eq(ExpressionComparator):
    """Simple comparator for single columns values that tests for equality with
    a given value (expression).
    """
    def __init__(
        self, lhs_expression, rhs_expression, ignore_case=False,
        raise_error=False
    ):
        """Create an instance of a single column equality predicate. The type
        of the returned object depends on whether the given value is a constant
        or a callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Eq, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )
        self.ignore_case = ignore_case

    def comp(self, left_values, right_values):
        """Compare two values and return True if they are equal.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            if isinstance(left_values, tuple):
                left_values = tuple([v.lower() for v in left_values])
            else:
                left_values = left_values.lower()
            if isinstance(right_values, tuple):
                right_values = tuple([v.lower() for v in right_values])
            else:
                right_values = right_values.lower()
        return left_values == right_values


class EqIgnoreCase(Eq):
    """Shortcut for comparing single column values in a case-insenstive manner.
    """
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Create an instance of the equality comparator with the ignore case
        flag set to True.

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(EqIgnoreCase, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            ignore_case=True,
            raise_error=raise_error
        )


class Geq(ExpressionComparator):
    """Simple comparator for single column values that tests if a column value
    is greater or equal than a given constant value or a value expression.
    """
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Create an instance of the greater than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Geq, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )

    def comp(self, left_values, right_values):
        """Return True if the left value is greater or equal than the right
        value.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_values >= right_values


class Gt(ExpressionComparator):
    """Simple comparator for single column values that tests if a column value
    is greater than a given constant value or a value expression.
    """
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Create an instance of the greater than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Gt, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )

    def comp(self, left_values, right_values):
        """Return True if the left value is greater than the right value.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_values > right_values


class Leq(ExpressionComparator):
    """Simple comparator for single column values that tests if a column value
    is less or equal than a given constant value or a value expression.
    """
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Create an instance of the less or equal comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Leq, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )

    def comp(self, left_values, right_values):
        """Return True if the left value is lower or equal that the right
        value.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_values <= right_values


class Lt(ExpressionComparator):
    """Simple comparator for single column values that tests if a column value
    is less than a given constant value or a value expression.
    """
    def __init__(self, lhs_expression, rhs_expression, raise_error=False):
        """Create an instance of the less than comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Lt, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )

    def comp(self, left_values, right_values):
        """Return True if the left value is lower than the right value.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_values < right_values


class Neq(ExpressionComparator):
    """Simple comparator for single column values that tests if a column value
    is not equal to a given constant value or a value expression.
    """
    def __init__(
        self, lhs_expression, rhs_expression, ignore_case=False,
        raise_error=False
    ):
        """Create an instance of the not equal comparator. The type of the
        returned object depends on whether the given value is a constant or a
        callable (value expression).

        Parameters
        ----------
        lhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs_expression: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Neq, self).__init__(
            lhs_expression=lhs_expression,
            rhs_expression=rhs_expression,
            raise_error=raise_error
        )
        self.ignore_case = ignore_case

    def comp(self, left_values, right_values):
        """Compare two values and return True if they are not equal.

        Parameters
        ----------
        left_values: scalar or tuple
            Left value of the comparison.
        right_values: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            if isinstance(left_values, tuple):
                left_values = tuple([v.lower() for v in left_values])
            else:
                left_values = left_values.lower()
            if isinstance(right_values, tuple):
                right_values = tuple([v.lower() for v in right_values])
            else:
                right_values = right_values.lower()
        return left_values != right_values


# -- Helper Functions ---------------------------------------------------------

def to_eval(value):
    """Ensure that the value is an evaluation function. If the given argument
    is not an evaluation function the value is wrapped as a constant value.

    Parameters
    ----------
    value: openclean.function.eval.base.EvalFunction or scalar
        Value that is represented as an evaluation function.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    if not isinstance(value, EvalFunction):
        value = Const(value)
    return value
