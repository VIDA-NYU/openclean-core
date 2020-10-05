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

from openclean.data.select import select_clause
from openclean.function.value.base import CallableWrapper, ValueFunction

import openclean.util as util


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
        openclean.function.eval.base.Add
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
        openclean.function.eval.base.Eq
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
        openclean.function.eval.base.FloorDivide
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
        openclean.function.eval.base.Gt
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
        openclean.function.eval.base.Geq
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
        openclean.function.eval.base.Leq
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
        openclean.function.eval.base.Lt
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
        openclean.function.eval.base.Multiply
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
        openclean.function.eval.base.Neq
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
        openclean.function.eval.base.Subtract
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
        openclean.function.eval.base.Divide
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


class PreparedFullRowEval(PreparedFunction):
    """Eval function that evaluates a callable on all values in a data frame
    row.
    """
    def __init__(self, func):
        """Initialize the callable. Raises a ValueError if the function
        argument is not a callable.

        Parameters
        ----------
        func: callable
            Callable that is evaluated on the list of cell values from a data
            frame row.

        Raises
        ------
        ValueError
        """
        # Raise ValueError if the function is not callable.
        self.func = util.ensure_callable(func)

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
        return self.func(*list(values))


class Eval(EvalFunction):
    """Wrapper around a callable function. Allows to apply a given function
    on the results of a list of values generated by evaluation functions from
    cells in data frame rows.
    """
    def __init__(self, columns, func, is_unary=None):
        """Initialized the nested producer function and the function that is
        being applied on the results of the producer. The is_unary flag
        indicates if the applied function requires tuples or lists returned by
        the producer to be cast to variable argument lists.

        Raises a ValueError if the producer is not a EvalFunction or the
        applied function is not a callable.

        Parameters
        ----------
        columns: list, tuple, or openclean.function.eval.base.EvalFunction
            Evaluation function to extract values from data frame rows. This
            can also be a list or tuple of evaluation functions or a list of
            column names or index positions.
        func: callable
            Callable that is applied on the values that are returned by the
            producer.
        is_unary: bool, default=None[=True]
            Control behavior for producer that return values which are lists or
            tuples.

        Raises
        ------
        ValueError
        """
        # Ensure that producer contains only evaluation functions.
        if isinstance(columns, tuple):
            producer = tuple([to_column_eval(c) for c in columns])
            is_unary = False if is_unary is None else is_unary
        elif isinstance(columns, list):
            producer = [to_column_eval(c) for c in columns]
            is_unary = False if is_unary is None else is_unary
        elif not isinstance(columns, EvalFunction):
            producer = Col(columns)
            is_unary = True if is_unary is None else is_unary
        else:
            producer = columns
            is_unary = True if is_unary is None else is_unary
        # If the function is an unary function we ensure that it is a
        # ValueFunction.
        if is_unary and not isinstance(func, ValueFunction):
            func = CallableWrapper(func)
        self.func = func
        self.producer = producer
        self.is_unary = is_unary

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
        if isinstance(self.producer, tuple):
            args = tuple([f.eval(values) for f in self.producer])
        elif isinstance(self.producer, list):
            args = [f.eval(values) for f in self.producer]
        else:
            args = self.producer.eval(values)
        if self.is_unary:
            return self.func.eval(args)
        elif not util.is_list_or_tuple(args):
            return self.func(args)
        else:
            return self.func(*args)

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
        # Prepare the function only if it is an unprepared value function.
        prepared_func = None
        if not isinstance(self.func, ValueFunction) or self.func.is_prepared():
            prepared_func = self.func
        if isinstance(self.producer, tuple):
            prepared_producer = tuple([f.prepare(df) for f in self.producer])
            if not prepared_func:
                # Generate the list of values to prepare the associated value
                # function.
                values = list()
                for _, row in df.iterrows():
                    args = tuple([f.eval(row) for f in prepared_producer])
                    values.append(args)
                prepared_func = self.func.prepare(values)
        elif isinstance(self.producer, list):
            prepared_producer = [f.prepare(df) for f in self.producer]
            if not prepared_func:
                # Generate the list of values to prepare the associated value
                # function.
                values = list()
                for _, row in df.iterrows():
                    values.append([f.eval(row) for f in prepared_producer])
                prepared_func = self.func.prepare(values)
        else:
            prepared_producer = self.producer.prepare(df)
            if not prepared_func:
                # Generate the list of values to prepare the associated value
                # function.
                values = list()
                for _, row in df.iterrows():
                    values.append(prepared_producer.eval(row))
                prepared_func = self.func.prepare(values)
        return Eval(
            func=prepared_func,
            columns=prepared_producer,
            is_unary=self.is_unary
        )


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


# -- Column function ----------------------------------------------------------

class Col(EvalFunction):
    """Evaluation function that returns value(s) from one or more column(s) in
    the data frame row.
    """
    def __init__(self, columns, colidx=None):
        """Initialize the source column(s).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        colidx: list(int), default=None
            Prepared list of index positions for columns.
        """
        self.columns = columns
        self._colidx = colidx

    def eval(self, values):
        """Get value from the lookup columns.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        if len(self._colidx) == 1:
            return values[self._colidx[0]]
        else:
            return [values[i] for i in self._colidx]

    def prepare(self, df):
        """Get index positions of the value columns for the schema of the
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        _, colidx = select_clause(df, columns=self.columns)
        return Col(columns=self.columns, colidx=colidx)


class Cols(EvalFunction):
    """Evaluation function that returns a tuple of values from one or more
    column(s) in the data frame row.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the source column(s).

        Parameters
        ----------
        args: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        colidx: list(int), default=None
            Prepared list of index positions for columns.
        """
        self.columns = args
        self._colidx = kwargs.get('colidx', None)

    def eval(self, values):
        """Get value from the lookup columns.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        if len(self._colidx) == 1:
            return values[self._colidx[0]]
        else:
            return tuple([values[i] for i in self._colidx])

    def prepare(self, df):
        """Get index positions of the value columns for the schema of the
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        _, colidx = select_clause(df, columns=list(self.columns))
        return Cols(*self.columns, colidx=colidx)


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
        self.lhs = to_const_eval(lhs)
        self.rhs = to_const_eval(rhs)
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
        return BinaryOperator(
            lhs=self.lhs.prepare(df),
            rhs=self.rhs.prepare(df),
            op=self.op,
            raise_error=self.raise_error,
            default_value=self.default_value
        )


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

def is_var_func(columns=None):
    """Helper function that returns True if the given column argument will
    result in an evaluation function that operates on a variable number of
    arguments.

    If the column argument is missing the result is False since the call to
    the evaluation function will contain the data frame row as the only
    argument.

    Parameters
    ----------
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
        These are the columns on which the function will be evaluated.
        If not specified the function is evaluated on the list of values
        in the data frame row (i.e., a data series object).

    Returns
    -------
    bool
    """
    if columns is None:
        # If no columns are specified the evaluation function will receive the
        # data frame row as the only argument.
        return False
    if isinstance(columns, list):
        # Lists with more than one element will create multiple arguments.
        return len(columns) > 1
    # Single column case
    return False


def to_const_eval(value):
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


def to_column_eval(value):
    """Convert a value into an evaluation function. If the value s not already
    an evaluation function, a column evaluation function is returned.

    Parameters
    ----------
    values: string, int, or openclean.function.eval.base.EvalFunction
        Value that is converted to an evaluation function.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    if not isinstance(value, EvalFunction):
        return Col(value)
    return value


def to_eval(value):
    """Convert a value into an evaluation function. If the value s not already
    an evaluation function, a full row evaluation function is returned.

    Parameters
    ----------
    values: string, int, or openclean.function.eval.base.EvalFunction
        Value that is converted to an evaluation function.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    if not isinstance(value, EvalFunction):
        return PreparedFullRowEval(func=value)
    return value
