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

from typing import Callable, List, Union, Optional

from openclean.data.types import Column
from openclean.data.select import select_clause
from openclean.function.value.base import CallableWrapper, ValueFunction


# -- Evaluation Functions -----------------------------------------------------

class EvalFunction(metaclass=ABCMeta):
    """Evaluation functions are used to compute results over rows in a data
    frame. Evaluation functions are evaluated for each row in a data frame and
    are expected to return a single scalar value or a list/tuple of values.

    Evaluation functions may be prepared in that they compute statistics
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


class PreparedEvalFunction(EvalFunction):
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


class FullRowEval(PreparedEvalFunction):
    """Evaluation function that passes each data frame row as an argument to a
    given consumer function. Note that neither the evaluation function nor the
    consumer will get prepared.
    """
    def __init__(self, func: Callable):
        """Initialize the consumer function.

        Parameters
        ----------
        func: callable
            Consumer that is called with each data frame row as argument to
            produce the result for this function.
        """
        self.consumer = func

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
        return self.consumer(values)


# Type alias for a single input column.
InputColumn = Union[int, str, Column, EvalFunction]


class TernaryEvalFunction(PreparedEvalFunction):
    """A ternary evaluation function extracts values from multiple columns in
    a data frame. It passes these values to a consumer function to generate the
    function result.

    The consumer may either be a unary or a ternary function. An unary function
    will receive a tuple of extracted values as the argument.

    Consumers may return a single (scalar) value or a tuple of values.
    """
    def __init__(
        self, columns: List[InputColumn], func: Union[Callable, ValueFunction],
        is_unary: Optional[bool] = False
    ):
        """Initialize the list of columns from which values are extracted and
        the consumer function. The input columns may eitehr be referenced by
        index position (in a data frame schema) or name. Inputs may also be
        generated by an evaluation function.


        Parameters
        ----------
        columns: list of input columns
            Specifies the columns from which values are extracted. This is
            expected to be a list of the following elements: (i) integers
            referencing the input column by index (in the data frame schema),
            (ii) strings giving the column name, or (iii) evaluation functions.
        func: callable or ValueFunction
            Callable (consumer) that is applied on the extracted values. This
            function is either a unary or ternary function (as specified by the
            `is_unary` flag).
        is_unary: bool, default=False
            Determines whether the consumer expects a single value or multiple
            values as argument. By default a ternary consumer is expected for
            a ternary evaluation function.
        """
        # Convert all elements in the input list to evaluation functions that
        # extract values from the columns of a data frame.
        self.producers = list()
        for val in columns:
            if not isinstance(val, EvalFunction):
                val = Col(val)
            self.producers.append(val)
        # Note that we do not enforce the consumer function to be a instance of
        # ValueFunction since these functions do not accept ternary argument
        # lists. If the user needs to prepare a ternaty consumer they have to
        # wrap their function inside the implementation of a ValueFunction and
        # pass an instance of that class as an argument here.
        self.consumer = func
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
        # Extract a tuple of argument values from the data frame row.
        args = tuple([f.eval(values) for f in self.producers])
        # Pass arguments to consumer either as single tuple (for unary
        # consumers) or as a variable argument list (for ternary consumers).
        if self.is_unary:
            return self.consumer(args)
        else:
            return self.consumer(*args)

    def prepare(self, df):
        """Prepare the producer and the consumer for a given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.UnaryEvalFunction
        """
        prepared_producers = [f.prepare(df) for f in self.producers]
        prepared_consumer = self.consumer
        # If the consumer is a value function that needs to be prepared we have
        # to extract the list of values from the data frame to prepare the
        # consumer.
        if isinstance(prepared_consumer, ValueFunction):
            if not prepared_consumer.is_prepared():
                # Generate the list of values to prepare the associated value
                # function.
                values = list()
                for _, row in df.iterrows():
                    values.append([f.eval(row) for f in prepared_producers])
                prepared_consumer = self.func.prepare(values)
            # Ensure that the producers are 'fresh' prepared in case that any
            # of them maintains an internal state.
            prepared_producers = [f.prepare(df) for f in self.producers]
        return TernaryEvalFunction(
            columns=prepared_producers,
            func=prepared_consumer,
            is_unary=self.is_unary
        )


class UnaryEvalFunction(PreparedEvalFunction):
    """An unary evaluation function extracts values from a single column in a
    data frame and passes the value on to an unary consumer.
    """
    def __init__(
        self, column: InputColumn, func: Union[Callable, ValueFunction]
    ):
        """Initialize the input column or evaluation function that is used to
        extract a value from data frame rows as well as the consumer that is
        called to process the extracted value.

        Raises a TypeError if the column argument does not specify a valid
        input column (or evaluation function) or if the consumer is not a
        callable or a value function.

        Parameters
        ----------
        column: single input column
            Specifies the column from which values are extracted. There are
            three different options to specify the input column: (i) as an
            integer referencing the input column by index (in the data frame
            schema), (ii) as a string giving the column name, or (iii) as an
            evaluation function that is expected to return a single value.
        func: callable or ValueFunction
            Callable (consumer) that is applied on the extracted values. This
            function is expected to be a unary function.

        Raises
        ------
        TypeError
        """
        # The producer is a evaluation function that will be used to extract
        # a single value from each data frame row. We use a single column
        # evaluation function as producer if the given argument references
        # a column in a data frame schema.
        if isinstance(column, EvalFunction):
            self.producer = column
        else:
            self.producer = Col(column)
        # The consumer if a value function. We use a callable wrapper if the
        # given argument is not a value function.
        if isinstance(func, ValueFunction):
            self.consumer = func
        else:
            self.consumer = CallableWrapper(func)  # noqa: E501

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
        return self.consumer.eval(self.producer.eval(values))

    def prepare(self, df):
        """Prepare the producer and the consumer for a given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.UnaryEvalFunction
        """
        # Get a prepared producer.
        prepared_producer = self.producer.prepare(df)
        # If the consumer has to be prepared we need to extract the list of
        # values from the data frame to prepare the consumer.
        if not self.consumer.is_prepared():
            values = list()
            for _, row in df.iterrows():
                values.append(prepared_producer.eval(row))
            prepared_consumer = self.consumer.prepare(values)
            # Make sure to get a fresh copy of the prepared producer in case
            # that the object maintains an internal state that has been
            # modified when generating the value list for preparing the
            # consumer.
            prepared_producer = self.producer.prepare(df)
        else:
            prepared_consumer = self.consumer
        return UnaryEvalFunction(
            column=prepared_producer,
            func=prepared_consumer
        )


# -- Factory ------------------------------------------------------------------


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

    The Eval factory itself only handles the preparation of the encapsuled
    consumer. It does not implement the eval() function of the super class.
    Instead, the prepare() function returns an instance of either a unary or
    ternary evaluation function that implement eval().
    """
    def __init__(
        self, columns: Union[InputColumn, List[InputColumn]],
        func: Union[Callable, ValueFunction], is_unary: Optional[bool] = None
    ):
        """Create an instance of an evaluation function that extractes values
        from the specified columns and passes the extracted values to a given
        consumer.

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
        # The type of evaluation function is determined based on the type of
        # the columns argument.
        if isinstance(columns, list):
            # By default we assume that the consumer for a ternary evaluation
            # function is ternary as well unless specified otherwise via the
            # is_unary flag.
            self.is_unary = is_unary if is_unary is not None else False
        elif is_unary is not None and not is_unary:
            # Raise TypeError if inputs are unary but the consumer is ternary.
            raise TypeError('cannot call ternary consumer with unary input')
        self.columns = columns
        self.func = func

    def eval(self, values):
        """The eval function of this class is not implemented. It is assumed
        that each evaluation function will be prepared before the first call
        to eval(). The prepare() function for this class should return an
        instance of an evaluation function with an implemented eval().

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        raise NotImplementedError('Eval function has not been prepared')

    def prepare(self, df):
        """Prepare the producer(s) and the consumer for a given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        # The type of the returned evaluation function is determined based on
        # the type of the columns argument.
        if isinstance(self.columns, list):
            func = TernaryEvalFunction(
                columns=self.columns,
                func=self.func,
                is_unary=self.is_unary
            )
        else:
            func = UnaryEvalFunction(column=self.columns, func=self.func)
        # Return a perpared version of the instantiated evaluation function.
        return func.prepare(df)


# -- Constant function --------------------------------------------------------

class Const(PreparedEvalFunction):
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
    """Evaluation function that returns the value from a single column in a
    data frame row.
    """
    def __init__(self, columns: InputColumn, colidx: Optional[int] = None):
        """Initialize the source column.

        Parameters
        ----------
        columns: int, string, or Column
            Single column specified either via the column index position in the
            data frame schema or the column name.
        colidx: list(int), default=None
            Index position for the source column if this function has been
            prepared.

        Raises
        ------
        TypeError
        """
        if not isinstance(columns, int) and not isinstance(columns, str):
            raise TypeError('invalid column specification {}'.format(columns))
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
        return values[self._colidx]

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
        return Col(columns=self.columns, colidx=colidx[0])


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
