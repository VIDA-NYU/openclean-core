# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data frame manipulating functions. Evaluation functions are
applied to one or more columns in a data frame. Functions are expected to
return either a data series or a list of scalar values.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, List, Union, Optional

import operator
import pandas as pd

from openclean.data.stream.base import DataRow, StreamFunction
from openclean.data.schema import column_ref, select_clause
from openclean.data.types import Column, Columns, Scalar, DatasetSchema, Value
from openclean.function.value.base import ValueFunction
from openclean.util.core import scalar_pass_through, tenary_pass_through


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
      receives the full data frame as an argument. It returns a data series
      (or list) of values with one value for each row in the input data frame.
      Functions that operate over multiple columns will return a list of
      tuples.
    - prepare: If an evaluation function is used as part of a data stream
      operator the function needs to be prepared. That is, the function will
      need to know the schema of the rows in the data frame before streaming
      starts. The prepare method receives the schema of the data stream as an
      argument. It returns a callable function that accepts a data stream row
      as the only argument and that returns a single value or a tuple of values
      depending on whether the evaluation function operators on one or more
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
        """Return an instance of the FloorDivide class to compute the floor
        division of two evaluation function values.

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
        """Return an instance of the Gt class to compare the results of two
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
        """Return an instance of the Geq class to compare the results of two
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
        """Return an instance of the Leq class to compare the results of two
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
        """Return an instance of the Lt class to compare the results of two
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
        """Return an instance of the Multiply class to compute the product
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
        """Return an instance of the Neq class to compare the results of two
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

    def __pow__(self, other: EvalSpec) -> EvalFunction:
        """Return an instance of the Pow class to compute to compute the value
        of this expression to the power of other.

        Parameters
        ----------
        other: scalar, callable, or opencelan.function.base.EvalFunction
            Right-hand side value of the operator.

        Returns
        -------
        openclean.function.eval.base.Pow
        """
        return Pow(self, other)

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
        """Return an instance of the Divide class to compute the division of
        two evaluation function values.

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
        a data series or a list of values. The resulting data contains one
        output value per input row. If the evaluation function operates over
        multiple columns then the result will be a list of tuples with the size
        of each tuple matching the number of columns the function operates on.

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
    def prepare(self, columns: DatasetSchema) -> StreamFunction:
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


"""Type aliases for parameters and return values of evaluation functions."""
EvalResult = Union[pd.Series, List[Value]]
EvalSpec = Union[Scalar, Callable, EvalFunction]
InputColumn = Union[int, str, Column, EvalFunction]
ValueExpression = Union[Scalar, EvalFunction]


# -- Factory ------------------------------------------------------------------

class BinaryStreamFunction(object):
    """Binary operator for data streams. Evaluates a given binary function on
    the result of two stream functions.
    """
    def __init__(self, lhs: StreamFunction, rhs: StreamFunction, op: Callable):
        """Initialize the column(s) (lhs and rhs) whose values are input for
        the binary operator.

        Parameters
        ----------
        lhs: openclean.data.stream.base.StreamFunction
            Stream function for left-hand-size value(s) of the operator.
        rhs: openclean.data.stream.base.StreamFunction
            Stream function for right-hand-side value(s) of the operator.
        op: callable
            Callable that accepts two arguments, the left-hand side and
            right-hand side values.
        """
        self.lhs = lhs
        self.rhs = rhs
        self.op = op

    def __call__(self, row: DataRow) -> Scalar:
        """Make the object a stream function. Returns the result of evaluating
        the operator on the row values that are extracted by the left-hand-side
        and right-hand-side stream functions.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        scalar
        """
        return self.op(self.lhs(row), self.rhs(row))


class TernaryStreamFunction(object):
    """A ternary stream function extracts values using multiple producers and
    passes them to a single consumer. The consumer may either be a unary or a
    ternary function. An unary function will receive a tuple of extracted
    values as the argument.
    """
    def __init__(
        self, producers: StreamFunction, consumer: Callable,
        is_unary: Optional[bool] = False
    ):
        """Initialize the list of producers and the consumer for values that
        are extracted (by the producers) from data stream rows.


        Parameters
        ----------
        producers: list of openclean.data.stream.base.StreamFunction
            List of stream functions that are used to extract values from data
            stream rows.
        consumer: callable
            Callable (consumer) that is applied on the extracted values. This
            function is either a unary or ternary function (as specified by the
            `is_unary` flag).
        is_unary: bool, default=False
            Determines whether the consumer expects a single value or multiple
            values as argument. By default a ternary consumer is expected for
            a ternary evaluation function.
        """
        self.producers = producers
        self.consumer = consumer
        self.is_unary = is_unary

    def __call__(self, row: DataRow) -> Scalar:
        """Make the object a stream function. Returns the result of evaluating
        the consumer on the row value that is extracted by the producer.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        scalar
        """
        # Extract values from the data stream row using the given producers.
        values = tuple([f(row) for f in self.producers])
        # Pass extracted values to the consumer to compute the function result.
        # For n-ary consumers we need to unpack the argument values.
        if self.is_unary:
            return self.consumer(values)
        else:
            return self.consumer(*values)


class UnaryStreamFunction(object):
    """Unary operator for data streams. Evaluates a given unary function on
    the result of another stream function.
    """
    def __init__(self, producer: StreamFunction, consumer: Callable):
        """Initialize the producer and consumer for data stream rows. The
        producer extracts a value from a given data frame row that is then
        passed on to the consumer to compute the function result.

        Parameters
        ----------
        producer: openclean.data.stream.base.StreamFunction
            Stream function for extracting values from data stream rows.
        rhs: openclean.data.stream.base.StreamFunction
            Stream function for right-hand-side value(s) of the operator.
        consumer: callable
            Callable that accepts two arguments, the left-hand side and
            right-hand side values.
        """
        self.producer = producer
        self.consumer = consumer

    def __call__(self, row: DataRow) -> Scalar:
        """Make the object a stream function. Returns the result of evaluating
        the consumer on the row value that is extracted by the producer.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        scalar
        """
        return self.consumer(self.producer(row))


class Eval(EvalFunction):
    """Eval is a factory for evaluation functions that extract values from one
    or more columns in data frame rows and that evaluate a given function
    (consumer) on the extracted values.

    We distinguish between unary evaluation functions that extract values from
    a single column and ternary evaluation functions that extract values from
    two or more columns. For the consumer we also distinguish between unary and
    ternary functions.

    The arity of an evaluation function is detemined by the number of input
    columns that are specified when calling the Eval factory. The arity of the
    consumer cannot be determined automatically but has to be specified by the
    user in the `is_unary` parameter.

    A ternary evaluation function with a unary consumer will pass a tuple with
    the extracted values to the consumer. A unary evaluation function with a
    ternary consumer will raise a TypeError error in the constructor.
    """
    def __init__(
        self, columns: Union[InputColumn, List[InputColumn]],
        func: Union[Callable, ValueFunction], args: Optional[Dict] = None,
        is_unary: Optional[bool] = None
    ):
        """Create an instance of an evaluation function that extracts values
        from the specified columns and applies a given function (consumer) on
        the extracted values.

        The `is_unary` flag indicates if the consumer expects a single argument
        value or a tuple of values.

        Raises a TypeError if the list of inputs specifies a single column only
        but the consumer expects a ternary input.

        Parameters
        ----------
        columns: single input column or list of input columns
            Specifies the column(s) from which values are extracted that are
            then passed to the given function (func) for processing. There are
            several different ways of specifying input columns. The type of an
            argument value for this parameter may either be (i) an integer
            referencing the input column by index (in the data frame schama),
            (ii) a string giving the column name, (iii) an evaluation function
            that is expected to return a single value, or (iv) a list of either
            (i)-(iii). If the argument type is a list a ternary evaluation
            function will be returned. Otherwise, the result is a unary
            evalaution function.
        func: callable or ValueFunction
            Callable (consumer) that is applied on the values that are returned
            by the producer. Whether this function is a unary or ternary
            function has to be specified by the user in the `is_unary` flag.
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        is_unary: bool, default=None
            Determines whether the consumer expects a single value or multiple
            values as argument. The default is None and the flag is ignored for
            unary evaluation functions. For ternary evaluation functions by
            default it is assumed that the consumer is a ternary function as
            well.

        Raises
        ------
        TypeError
        """
        # Generate a list of producers. Producers are evaluation functions that
        # generate the input values for the consumer.
        self.producers = list()
        if isinstance(columns, list) or isinstance(columns, tuple):
            # By default we assume that the consumer for a ternary evaluation
            # function is ternary as well unless specified otherwise via the
            # is_unary flag.
            self.is_unary = is_unary if is_unary is not None else False
            for c in columns:
                self.producers.append(to_column_eval(c))
        elif is_unary is not None and not is_unary:
            # Raise TypeError if inputs are unary but the consumer is ternary.
            raise TypeError('cannot call ternary consumer with unary input')
        else:
            # Columns is a single value.
            self.producers.append(to_column_eval(columns))
            self.is_unary = True
        # Ensure that func is given. If the current value is None we set func
        # to either a unary function or a tenary function (depending on the
        # value of the is_unary flag).
        if func is None:
            func = scalar_pass_through if self.is_unary else tenary_pass_through
        # The consumer is either a value function or a callable. If it is a
        # value function it might require preparation. A callable will never
        # require preparation.
        self._is_prepared = func.is_prepared() if isinstance(func, ValueFunction) else True  # noqa: E501
        self.consumer = func
        self.args = args

    def decorate(self, func):
        """Decorate the given function with the optional keyword arguments that
        were given (if given) in the constructor for the Eval function.

        Parameters
        ----------
        func: callable
            Function that is being decorated.

        Returns
        -------
        callable
        """
        # Return undecorated function if no additional arguments are specified.
        if self.args is None:
            return func

        if len(self.producers) == 1 or self.is_unary:
            # Decorate a unary function that passes the static arguments as
            # keyword arguments to the decorated function.

            def decorated_unary_func(value):
                return func(value, **self.args)

            return decorated_unary_func

        else:
            # Decorate a ternary function that takes multiple column values as
            # input together with the static arguments that are passed on as
            # keyword arguments to the decorated function.

            def decorated_ternary_func(*value):
                return func(*value, **self.args)

            return decorated_ternary_func

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Evaluate the consumer on the lists of values that are generated by
        the referenced columns.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        pd.Series or list
        """
        # We distinguish three main cases based on the number of producers and
        # the arity of the consumer.
        if len(self.producers) == 1:
            # The input values for the consumer come from a single producer.
            data = self.producers[0].eval(df)
            # Prepare the consumer if necessary.
            if not self._is_prepared:
                prep_consumer = self.consumer.prepare(data)
            else:
                prep_consumer = self.consumer
            func = self.decorate(prep_consumer)
            return [func(v) for v in data]
        else:
            # Inputs for the consumer come from multiple producers. Start with a
            # list of lists by evaluating the producers on the given data frame.
            data = [f.eval(df) for f in self.producers]
            # Prepare the consumer if necessary.
            if not self._is_prepared:
                prep_consumer = self.consumer.prepare([t for t in zip(*data)])
            else:
                prep_consumer = self.consumer
            func = self.decorate(prep_consumer)
            # Iterate over all result tuples and pass them to the consumer. The
            # implementation differes depending on the arity of the consumer.
            # A unary consumer receives a tuple of values. For a n-ary consumer
            # we need to unpack the tuple.
            if self.is_unary:
                return [func(v) for v in zip(*data)]
            else:
                return [func(*t) for t in zip(*data)]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
        """Create a stream function that applies the consumer on the results
        from one or more stream functions for the producers.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        # Raises an error if the consumer needs to be prepared. Value function
        # preparation is not supported for data streams since we do not have
        # access to the full list of values at this point.
        if not self._is_prepared:
            raise RuntimeError('cannot prepare value function for stream')
        # Create stream functions for all producers.
        prep_prods = [f.prepare(columns) for f in self.producers]
        # Distinguish between unary and ternary stream functions based on the
        # number of producers.
        func = self.decorate(self.consumer)
        if len(prep_prods) == 1:
            return UnaryStreamFunction(
                producer=prep_prods[0],
                consumer=func
            )
        else:
            return TernaryStreamFunction(
                producers=prep_prods,
                consumer=func,
                is_unary=self.is_unary
            )


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
        if isinstance(value, list):
            self.value = tuple(value)
        else:
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

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
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
        return df.iloc[:, colidx]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
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
    column(s) in the data frame row. Extends the abstract evaluation function
    and implements the stream function interface. For a stream function the
    internal _colidxs have to be defined (given at object construction).
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
        if len(self._colidxs) == 1:
            return values[self._colidxs[0]]
        else:
            return tuple([values[i] for i in self._colidxs])

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Get the values from the data frame columns that are referenced by
        this function. Returns a list of tuples with one value for each of
        the referenced columns.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        list
        """
        _, colidxs = select_clause(schema=df.columns, columns=self.columns)
        # Generating the tuples over the resulting data differs depending on
        # whether one or multiple columns are referenced.
        if len(colidxs) == 1:
            # For a single column we iterate over a data series.
            return [(v, ) for v in df.iloc[:, colidxs][0]]
        else:
            # For multiple columns we iterate over a data frame.
            data = df.iloc[:, colidxs]
            return [tuple(r) for r in data.itertuples(index=False, name=None)]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
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
    """Generic operator for comparing or transforming two column value
    expressions (that are represented as evaluation functions).
    """
    def __init__(
        self, lhs: ValueExpression, rhs: ValueExpression, op: Callable
    ):
        """Initialize the two value expressions and the binary operator. For
        the value expressions evaluation functions are expected.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        op: callable
            Callable that accepts two arguments, the left-hand side and
            right-hand side values.
        """
        self.lhs = to_const_eval(lhs)
        self.rhs = to_const_eval(rhs)
        self.op = op

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Evaluate the binary operator on a given data frame. The result is
        either as single data series or a list of scalarn values.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        pd.Series or list
        """
        # Extract values for lhs and rhs of the comparison from the data frame.
        lhs_data = self.lhs.eval(df)
        rhs_data = self.rhs.eval(df)
        # Evaluation of the comparison depends on whether the lhs and rhs are
        # both data series or not.
        if isinstance(lhs_data, pd.Series) and isinstance(rhs_data, pd.Series):
            # We can apply the operator directly on the two data series.
            return self.op(lhs_data, rhs_data)
        else:
            # Iterate over the values in the two results and apply the
            # comparison operator to each pair of values.
            return [self.op(v1, v2) for v1, v2 in zip(lhs_data, rhs_data)]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
        """Prepare both evaluation functions (lhs and rhs) and return a
        binary operator stream function.

        Parameters
        ----------
        columns: list of string
            Schema for data stream rows.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        return BinaryStreamFunction(
            lhs=self.lhs.prepare(columns),
            rhs=self.rhs.prepare(columns),
            op=self.op
        )


# -- Comparison predicates ----------------------------------------------------

class Eq(BinaryOperator):
    """Binary equality comparison predicate."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Eq, self).__init__(lhs=lhs, rhs=rhs, op=operator.eq)


class Geq(BinaryOperator):
    """Predicate for '>=' comparison."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Geq, self).__init__(lhs=lhs, rhs=rhs, op=operator.ge)


class Gt(BinaryOperator):
    """Predicate for '>' comparison."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Gt, self).__init__(lhs=lhs, rhs=rhs, op=operator.gt)


class Leq(BinaryOperator):
    """Predicate for '<=' comparison."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Leq, self).__init__(lhs=lhs, rhs=rhs, op=operator.le)


class Lt(BinaryOperator):
    """Predicate for '<' comparison."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Lt, self).__init__(lhs=lhs, rhs=rhs, op=operator.lt)


class Neq(BinaryOperator):
    """Predicate for '!=' comparison."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the comparison.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the comparison.
        """
        super(Neq, self).__init__(lhs=lhs, rhs=rhs, op=operator.ne)


# -- Arithmetic operators -----------------------------------------------------

class Add(BinaryOperator):
    """Arithmetic '+' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(Add, self).__init__(lhs=lhs, rhs=rhs, op=operator.add)


class Divide(BinaryOperator):
    """Arithmetic '/' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(Divide, self).__init__(lhs=lhs, rhs=rhs, op=operator.truediv)


class FloorDivide(BinaryOperator):
    """Arithmetic '//' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(FloorDivide, self).__init__(
            lhs=lhs,
            rhs=rhs,
            op=operator.floordiv
        )


class Multiply(BinaryOperator):
    """Arithmetic '*' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(Multiply, self).__init__(lhs=lhs, rhs=rhs, op=operator.mul)


class Pow(BinaryOperator):
    """Arithmetic '**' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(Pow, self).__init__(lhs=lhs, rhs=rhs, op=operator.pow)


class Subtract(BinaryOperator):
    """Arithmetic '-' operator."""
    def __init__(self, lhs: ValueExpression, rhs: ValueExpression):
        """Initialize the expressions of the operator.

        Parameters
        ----------
        lhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for left value(s) of the operator.
        rhs: scalar or openclean.function.eval.base.EvalFunction
            Value expression for right value(s) of the operator.
        """
        super(Subtract, self).__init__(lhs=lhs, rhs=rhs, op=operator.sub)


# -- Helper Functions ---------------------------------------------------------

def evaluate(df: pd.DataFrame, producers: List[EvalFunction]) -> EvalResult:
    """Helper method to extract a list of values (i.e., an evaluation result)
    from a data frame using one or more producers (evaluation functions).

    Results are generated by evaluating the given producers individually. If a
    single producer is given, the result from that producer will be returned.
    If multiple producers are given, a list of tuples with results from each
    consumer will be returned.

    Parameters
    ----------
    df: pd.DataFrame
        Pandas data frame.
    producers: list of openclean.function.eval.base.EvalFunctions
        List of evaluation functions that are used as data (series) producer.

    Returns
    -------
    pd.Series or list
    """
    if len(producers) == 1:
        # The input values for the consumer come from a single producer.
        return producers[0].eval(df)
    else:
        # Inputs for the consumer come from multiple producers.
        return [t for t in zip(*[f.eval(df) for f in producers])]


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
        if isinstance(value, list):
            value = tuple(value)
        value = Const(value)
    return value


def to_column_eval(value: InputColumn) -> EvalFunction:
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


"""Type alias for to_eval input parameters. Since the function can be used to
instantiate Col and Const functions (depending on the given factory) it has to
accepts either column references or scalars.
"""
Producer = Union[InputColumn, Scalar]


def to_eval(
    producers: Union[Producer, List[Producer]],
    factory: Optional[Callable] = to_column_eval
) -> List[EvalFunction]:
    """Convert a single input column or a list of input column into a list of
    evaluation functions. The optional factory function (cls) is used to create
    instances of an evaluation function for scalar argument values.

    Parameters
    ----------
    producers: int, string, EvaluationFunction, or list
        Specification of one or more input producers for an evaluation
        function.
    factory: callable
        Factory for evaluation functions that can be instantiated using a
        single scalar argument (e.g., Col or Const).

    Returns
    -------
    list
    """
    result = list()
    if isinstance(producers, list) or isinstance(producers, tuple):
        for c in producers:
            result.append(factory(c))
    else:
        result.append(factory(producers))
    return result
