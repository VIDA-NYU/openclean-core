# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for value generating functions. Evaluation functions are
applied to tuples (series) in a dataset (data frame). Functions are expected to
return either a scalar value or a tuple of scalar values.
"""

from abc import ABCMeta, abstractmethod


# -- Evaluation Functions -----------------------------------------------------

class EvalFunction(metaclass=ABCMeta):
    """Evaluation functions are used to compute results over rows in a data
    frame. Evaluation functions may be prepared in that they compute statistics
    or maintain column indices for the data frame proior to being evaluated.
    """
    def __add__(self, other):
        """Return an instance of the Add class to compute the sum of two
        evaluation function values.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.base.Add
        """
        return Add(self, other)

    def __call__(self, values):
        """Make the function callable.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        return self.eval(values)

    def __eq__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions for equality.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Eq
        """
        return Eq(self, other)

    def __floordiv__(self, other):
        """Return an instance of the Add class to compute the (floor) division
        of two evaluation function values.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.base.FloorDivide
        """
        return FloorDivide(self, other)

    def __gt__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions using '>'.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Gt
        """
        return Gt(self, other)

    def __ge__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions using '>='.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Geq
        """
        return Geq(self, other)

    def __le__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions using '<='.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Leq
        """
        return Leq(self, other)

    def __lt__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions using '<'.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Lt
        """
        return Lt(self, other)

    def __mul__(self, other):
        """Return an instance of the Subtract class to compute the product
        of two evaluation function values.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.base.Multiply
        """
        return Multiply(self, other)

    def __ne__(self, other):
        """Return an instance of the Eq class to compare the results of two
        evaluation functions for inequality.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the comparison.

        Returns
        -------
        openclean.function.base.Neq
        """
        return Neq(self, other)

    def __sub__(self, other):
        """Return an instance of the Subtract class to compute the subtraction
        of two evaluation function values.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.base.Subtract
        """
        return Subtract(self, other)

    def __truediv__(self, other):
        """Return an instance of the Add class to compute the division of two
        evaluation function values.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.base.Divide
        """
        return Divide(self, other)

    @abstractmethod
    def eval(self, values):
        """Evaluate the function on a given data frame row. The result type is
        implementation dependent. The result could either be a single scalar
        value or a tuple of scalar values.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        raise NotImplementedError()

    @abstractmethod
    def is_prepared(self):
        """Returns True if the evaluation function has been prepared and does
        not require a call of the prepare method.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def prepare(self, df):
        """Prepare the evaluation function before the first call to the eval()
        method for the given data frame. This allows to compute statistics or
        column indices for the data frame.

        Functions that need preparation should return a new instance of the
        evaluation function as the result of this method.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise NotImplementedError()


class PreparedFunction(EvalFunction):
    """Evaluation function that does not need to be prepared nor has it an
    associated function that would need preparation.
    row.
    """
    def is_prepared(self):
        """Always True for prepared functions.

        Returns
        -------
        bool
        """
        return True

    def prepare(self, df):
        """Since the function is prepared it can return a reference to itself.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        return self


# -- Constant function --------------------------------------------------------

class Const(PreparedFunction):
    """Evaluation function that returns a constant value for each data frame
    row.
    """
    def __init__(self, value):
        """Initialize the constant return value.

        Parameters
        ----------
        value: scalar
            Constant return value for the function.
        """
        self.value = value

    def eval(self, values):
        """Execute method for the evaluation function. Returns the defined
        constant value.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        return self.value


# -- Binary operators ---------------------------------------------------------

class BinaryOperator(EvalFunction):
    """Generic comparator for comparing two column value expressions."""
    def __init__(self, lhs, rhs, op, raise_error=False, default_value=None):
        """Initialize the column(s) (lhs) whose values are compared against the
        given value expression (rhs). For both arguments a evaluation function
        is expected.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        op: callable
            Callable that accepts two arguments, the left-hand side and
            right-hand side values.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        default_value: any, default=None
            Default value that is returned instead of raising an error.
        """
        self.lhs = to_eval(lhs)
        self.rhs = to_eval(rhs)
        self.op = op
        self.raise_error = raise_error
        self.default_value = default_value

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
            return self.op(self.lhs(values), self.rhs(values))
        except TypeError as ex:
            if self.raise_error:
                raise ex
            else:
                return self.default_value
        except AttributeError:
            return self.default_value

    def is_prepared(self):
        """Return True if both associated expressions are prepared.

        Returns
        -------
        bool
        """
        return self.lhs.is_prepared() and self.rhs.is_prepared()

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
        if not self.is_prepared():
            return BinaryOperator(
                lhs=self.lhs.prepare(df),
                rhs=self.rhs.prepare(df),
                op=self.op,
                raise_error=self.raise_error,
                default_value=self.default_value
            )
        else:
            return self


# -- Comparison predicates ----------------------------------------------------

class Eq(BinaryOperator):
    """Binary equality comparison predicate."""
    def __init__(self, lhs, rhs, ignore_case=False, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs_: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Eq, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )
        self.ignore_case = ignore_case

    def compare(self, lhs, rhs):
        """Compare two values and return True if they are equal.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            if isinstance(lhs, tuple):
                lhs = tuple([v.lower() for v in lhs])
            else:
                lhs = lhs.lower()
            if isinstance(rhs, tuple):
                rhs = tuple([v.lower() for v in rhs])
            else:
                rhs = rhs.lower()
        return lhs == rhs


class EqIgnoreCase(Eq):
    """Shortcut for comparing single column values in a case-insenstive manner.
    """
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs_: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(EqIgnoreCase, self).__init__(
            lhs=lhs,
            rhs=rhs,
            ignore_case=True,
            raise_error=raise_error
        )


class Geq(BinaryOperator):
    """Predicate for '>=' comparison."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Geq, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )

    def compare(self, lhs, rhs):
        """Return True if the left value is greater or equal than the right
        value.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return lhs >= rhs


class Gt(BinaryOperator):
    """Predicate for '>' comparison."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Gt, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )

    def compare(self, lhs, rhs):
        """Return True if the left value is greater than the right value.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return lhs > rhs


class Leq(BinaryOperator):
    """Predicate for '<=' comparison."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Leq, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )

    def compare(self, lhs, rhs):
        """Return True if the left value is lower or equal that the right
        value.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return lhs <= rhs


class Lt(BinaryOperator):
    """Predicate for '<' comparison."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Lt, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )

    def compare(self, lhs, rhs):
        """Return True if the left value is lower than the right value.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return lhs < rhs


class Neq(BinaryOperator):
    """Predicate for '!=' comparison."""
    def __init__(self, lhs, rhs, ignore_case=False, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Neq, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compare,
            raise_error=raise_error,
            default_value=False
        )
        self.ignore_case = ignore_case

    def compare(self, lhs, rhs):
        """Compare two values and return True if they are not equal.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the comparison.
        rhs: scalar or tuple
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            if isinstance(lhs, tuple):
                lhs = tuple([v.lower() for v in lhs])
            else:
                lhs = lhs.lower()
            if isinstance(rhs, tuple):
                rhs = tuple([v.lower() for v in rhs])
            else:
                rhs = rhs.lower()
        return lhs != rhs


# -- Arithmetic operators -----------------------------------------------------

class Add(BinaryOperator):
    """Arithmetic '+' operator."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being added.
        """
        super(Add, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compute,
            raise_error=raise_error,
            default_value=0
        )

    def compute(self, lhs, rhs):
        """Compute sum of the given values.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the operator.
        rhs: scalar or tuple
            Right value of the operator.

        Returns
        -------
        scalar
        """
        return lhs + rhs


class Divide(BinaryOperator):
    """Arithmetic '/' operator."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being added.
        """
        super(Divide, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compute,
            raise_error=raise_error,
            default_value=0
        )

    def compute(self, lhs, rhs):
        """Compute division of the given values.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the operator.
        rhs: scalar or tuple
            Right value of the operator.

        Returns
        -------
        scalar
        """
        return lhs / rhs


class FloorDivide(BinaryOperator):
    """Arithmetic '//' operator."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being added.
        """
        super(FloorDivide, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compute,
            raise_error=raise_error,
            default_value=0
        )

    def compute(self, lhs, rhs):
        """Compute floor division of the given values.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the operator.
        rhs: scalar or tuple
            Right value of the operator.

        Returns
        -------
        scalar
        """
        return lhs // rhs


class Multiply(BinaryOperator):
    """Arithmetic '*' operator."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being added.
        """
        super(Multiply, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compute,
            raise_error=raise_error,
            default_value=0
        )

    def compute(self, lhs, rhs):
        """Compute product of the given values.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the operator.
        rhs: scalar or tuple
            Right value of the operator.

        Returns
        -------
        scalar
        """
        return lhs * rhs


class Subtract(BinaryOperator):
    """Arithmetic '-' operator."""
    def __init__(self, lhs, rhs, raise_error=False):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being added.
        """
        super(Subtract, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=self.compute,
            raise_error=raise_error,
            default_value=0
        )

    def compute(self, lhs, rhs):
        """Compute subtraction of the given values.

        Parameters
        ----------
        lhs: scalar or tuple
            Left value of the operator.
        rhs: scalar or tuple
            Right value of the operator.

        Returns
        -------
        scalar
        """
        return lhs - rhs


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
