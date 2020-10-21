# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data frame manipulating functions. Evaluation functions are
applied to one or more columns in a data frame. Functions are expected to
return either a data series or a list of scalar values.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Callable, List, Union, Optional

import pandas as pd

from openclean.data.stream.base import DataRow, StreamFunction
from openclean.data.select import column_ref, select_clause
from openclean.data.types import Column, Columns, Scalar, Schema, Value
from openclean.function.value.base import CallableWrapper, ValueFunction


# -- Evaluation Functions -----------------------------------------------------

class EvalFunction(metaclass=ABCMeta):
    """Evaluation functions are used to compute results over rows in a data
    frame or a data stream. Conceptually, evaluation functions are evaluated
    over one or more columns for each row in the input data. For each row, the
    function is expected to generate one (or more) (transformed) value(s) for
    the column (columns) on which it operates.

    Evaluation functions are building blocks for data frame operators as well
    as data stream pipelines. Each of these two use cases is supported by a
    different (abstract) method:

    - eval: The eval function is used by data frame operators. The function
      receives the full data frame as an anrgument. It returns a data series
      (or list) of values with one value for each row in the input data frame.
      Functions that operate over multiple columns will return a series with
      multiple columns.
    - prepare: If an evaluation function is used as part of a data stream
      operator the function needs to be prepared. That is, the function will
      need to know the schema of the rows in the data frame before streaming
      starts. The prepate method recived the data stream schema as an argument.
      It returns a callable function that accepts a data stream row as the
      only argument and that returns a single value or a tuple of values
      depending on whether the evaluation function operators on on or more
      columns.
    """
    def __add__(self, other: EvalSpec) -> EvalFunction:
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

    def __eq__(self, other: EvalSpec) -> EvalFunction:
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

    def __floordiv__(self, other: EvalSpec) -> EvalFunction:
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

    def __gt__(self, other: EvalSpec) -> EvalFunction:
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

    def __ge__(self, other: EvalSpec) -> EvalFunction:
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

    def __le__(self, other: EvalSpec) -> EvalFunction:
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

    def __lt__(self, other: EvalSpec) -> EvalFunction:
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

    def __mul__(self, other: EvalSpec) -> EvalFunction:
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

    def __ne__(self, other: EvalSpec) -> EvalFunction:
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

    def __sub__(self, other: EvalSpec) -> EvalFunction:
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

    def __truediv__(self, other: EvalSpec) -> EvalFunction:
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
    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Evaluate the function on a given data frame. The result is either
        a data series or a list of values. The length of the returned series
        is equal to the number of rows in the data frame (i.e., one output
        value per input row). If the evaluation function operates over multiple
        columns then the result will also have multiple columns (or be a tuple
        if the result is a list), with the number of output columns matching
        the number of columns the function operates on.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        pd.Series or list
        """
        raise NotImplementedError()

    @abstractmethod
    def prepare(self, columns: Schema) -> StreamFunction:
        """Prepare the evaluation function to be able to process rows in a data
        stream. This method is called before streaming starts to inform the
        function about the schema of the rows in the data stream.

        Prepare is expected to return a callable that accepts a single data
        stream row as input and that returns a single value (if the function
        operates on a single column) or a tuple of values (for functions that
        operate over multiple columns).

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        raise NotImplementedError()


# -- Constant function --------------------------------------------------------

class Const(EvalFunction):
    """Evaluation function that returns a constant value for each data frame
    row. Extends the abstract evaluation function and implements the stream
    function interface.
    """
    def __init__(self, value: Value):
        """Initialize the constant return value.

        Parameters
        ----------
        value: scalar or tuple
            Constant return value for the function.
        """
        self.value = value

    def __call__(self, row: DataRow) -> Value:
        """Make the object a stream function. Returns the constant value for
        every row that the function is called with.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        scalar or tuple
        """
        return self.value

    def eval(self, df: pd.DataFrame) -> List[Value]:
        """Execute method for the evaluation function. Returns a list in the
        length of the data frame (row count) with the defined constant value.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        list
        """
        return [self.value] * df.shape[0]

    def prepare(self, columns: Schema) -> StreamFunction:
        """The prepare method returns a callable that returns the constant
        value for evary input row.

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        return self


# -- Column functions ---------------------------------------------------------

class Col(EvalFunction):
    """Evaluation function that returns the value from a single column in a
    data frame row. Extends the abstract evaluation function and implements the
    stream function interface. For a stream function the internal _colidx has
    to be defined (given at object construction).
    """
    def __init__(self, column: InputColumn, colidx: Optional[int] = None):
        """Initialize the source column.

        Parameters
        ----------
        column: int, string, or Column
            Single column specified either via the column index position in the
            data frame schema or the column name.
        colidx: list(int), default=None
            Index position for the source column if this function has been
            prepared.

        Raises
        ------
        TypeError
        """
        if not isinstance(column, int) and not isinstance(column, str):
            raise TypeError('invalid column specification {}'.format(column))
        self.column = column
        self._colidx = colidx

    def __call__(self, row: DataRow) -> Scalar:
        """Make the object a stream function. Returns the cell value from the
        column that was specified at object construction.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        scalar
        """
        return row[self._colidx]

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Get the values from the data frame column that is referenced by this
        function.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        pd.Series
        """
        _, colidx = column_ref(schema=df.columns, column=self.column)
        return df.iloc[:, colidx[0]]

    def prepare(self, columns: Schema) -> StreamFunction:
        """Return a Col function that is prepared, i.e., that has the column
        index for the column that it operates on initialized.

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        _, colidx = column_ref(schema=columns, column=self.column)
        return Col(column=self.column, colidx=colidx)


class Cols(EvalFunction):
    """Evaluation function that returns a tuple of values from one or more
    column(s) in the data frame row.
    """
    def __init__(self, columns: Columns, colidxs: Optional[List[int]] = None):
        """Initialize the source column(s).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        colidxs: list(int), default=None
            Prepared list of index positions for columns.
        """
        # Ensure that columns is a list.
        self.columns = columns if isinstance(columns, list) else [columns]
        self._colidxs = colidxs

    def __call__(self, values):
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

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Get the values from the data frame column that is referenced by this
        function.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        pd.Series
        """
        _, colidxs = select_clause(schema=df.columns, columns=self.columns)
        return df.iloc[:, colidxs]

    def prepare(self, columns: Schema) -> StreamFunction:
        """Return a Cols function that is prepared, i.e., that has the column
        indexes for the columns that it operates on initialized.

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        _, colidxs = select_clause(schema=columns, columns=self.columns)
        return Cols(columns=self.columns, colidxs=colidxs)


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

    def filter(self, df):
        return self.lhs.filter(df) == self.rhs.filter(df)


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


"""Type aliases for parameters and return values of evaluation functions."""
EvalResult = Union[pd.Series, List[Value]]
EvalSpec = Union[Scalar, Callable, EvalFunction]
InputColumn = Union[int, str, Column, EvalFunction]
