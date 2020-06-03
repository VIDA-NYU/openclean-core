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

from openclean.data.column import select_clause
from openclean.data.sequence import Sequence
from openclean.function.value.base import CallableWrapper, ValueFunction


# -- Evaluation Functions -----------------------------------------------------

class EvalFunction(metaclass=ABCMeta):
    """Evaluation functions are used to compute results over rows (i.e., data
    series objects) in a data frame. Evaluation functions may be prepared in
    that they compute statistics or maintain column indices for the data frame
    proior to being evaluated.
    """
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

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        raise NotImplementedError()


class Eval(object):
    """Factory pattern for eval functions that evaluate a callable on rows in a
    data frame. The callable may be a prepared callable. The factory returns an
    eval function object from one of three different classes based on the value
    of the columns argument. The signature of the __call__ method for the
    encapsulated function is expected to have a signature that matches the
    number of columns the function is evaluated on.
    """
    def __new__(cls, func, columns=None):
        """Return an instance of the eval function depending on columns
        argument. Raises ValueError if the given functionis not callable.

        The constructors of the different eval function implementations will
        raise a ValueError if the given funciton is not a callable.

        Parameters
        ----------
        func: callable
            Expects a single callable
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.

        Raises
        ------
        ValueError
        """
        # If no columns are are given the function is evaluated on the full
        # data frame row.
        if columns is None:
            if isinstance(func, ValueFunction):
                return FullRowValueEval(func=func)
            else:
                return FullRowEval(func=func)
        # Return different instances of the eval function depending on whether
        # the function is applied on no column, one column or multiple columns.
        if isinstance(columns, list):
            # Even if columns is a list it may only contain a single element.
            if len(columns) == 1:
                if isinstance(func, ValueFunction):
                    return SingleColumnValueEval(func=func, columns=columns[0])
                else:
                    return SingleColumnEval(func=func, columns=columns[0])
            elif len(columns) > 1:
                if isinstance(func, ValueFunction):
                    return MultiColumnValueEval(func=func, columns=columns)
                else:
                    return MultiColumnEval(func=func, columns=columns)
            else:
                if isinstance(func, ValueFunction):
                    return FullRowValueEval(func=func)
                else:
                    return FullRowEval(func=func)
        else:
            if isinstance(func, ValueFunction):
                return SingleColumnValueEval(func=func, columns=columns)
            else:
                return SingleColumnEval(func=func, columns=columns)


class FullRowEval(EvalFunction):
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
        if not callable(func):
            raise ValueError('not a callable')
        self.func = func

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
        return self.func(values)

    def prepare(self, df):
        """If the evaluation function is a prepared callable the respective
        prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        # Prepare the callable if it is a prepared function.
        if isinstance(self.func, EvalFunction):
            self.func.prepare(df)
        return self


class FullRowValueEval(EvalFunction):
    """Eval function that evaluates a value function on all values in a data
    frame row.
    """
    def __init__(self, func):
        """Initialize the value function. Raises a ValueError if the function
        argument is not a callable or an instance of ValueFunction.

        Parameters
        ----------
        func: (openclean.function.value.base.ValueFunction or callable)
            Callable or value function that is evaluated on the list of cell
            values from a data frame row.

        Raises
        ------
        ValueError
        """
        # Ensure that the function is of type ValueFunction. Raises ValueError
        # if the function is not callable.
        if not isinstance(func, ValueFunction):
            func = CallableWrapper(func)
        self.func = func

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
        return self.func(tuple([values]))

    def prepare(self, df):
        """If the evaluation function is a prepared callable the respective
        prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        # Prepare the callable if it is a prepared function.
        if not self.funcis_prepared():
            self.func.prepare(Sequence(df=df, columns=[df.columns]))
        return self


class MultiColumnEval(EvalFunction):
    """Eval function that applies a callable on a set of columns (values) in a
    data frame row.
    """
    def __init__(self, func, columns):
        """Initialize the function and columns. Raises a ValueError if the
        given function is not a callable.

        Parameters
        ----------
        func: callable
            Callable that is being evaluated.
        columns: list(int or string)
            List of column index positions or column names.

        Raises
        ------
        ValueError
        """
        # Raise ValueError if the function is not callabel
        if not callable(func):
            raise ValueError('not a callable')
        self.func = func
        self.columns = columns
        # The list of column indices is initially None. the list will be
        # initialized by the prepare method.
        self.colidxs = None

    def eval(self, values):
        """Evaluate the callable on the given data frame row. Passes only the
        cell values from those columns that were specified when the object was
        instantiated.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        args = [values[c] for c in self.colidxs]
        return self.func(*args)

    def prepare(self, df):
        """Initialize the corresponding list of column indices for the columns
        that were specified at object instantiation. If the evaluation function
        is a prepared callable the respective prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        _, self.colidxs = select_clause(df=df, columns=self.columns)
        # Prepare the callable if it is a prepared function.
        if isinstance(self.func, EvalFunction):
            self.func.prepare(df)
        return self


class MultiColumnValueEval(EvalFunction):
    """Eval function that applies a value function on a set of columns (values)
    in a data frame row.
    """
    def __init__(self, func, columns):
        """Initialize the function and columns. Raises a ValueError if the
        given function is not a callable or of type ValueFunction.

        Parameters
        ----------
        func: (openclean.function.value.base.ValueFunction or callable)
            Callable or value function that is evaluated on a list of cell
            values from a data frame row.
        columns: list(int or string)
            List of column index positions or column names.

        Raises
        ------
        ValueError
        """
        # Ensure that the function is of type ValueFunction. Raises ValueError
        # if the function is not callable.
        if not isinstance(func, ValueFunction):
            func = CallableWrapper(func)
        self.func = func
        self.columns = columns
        # The list of column indices is initially None. the list will be
        # initialized by the prepare method.
        self.colidxs = None

    def eval(self, values):
        """Evaluate the callable on the given data frame row. Passes only the
        cell values from those columns that were specified when the object was
        instantiated.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        # The value function will expect a single argument. Make sure to create
        # a tuple containing the column values.
        return self.func(tuple([values[c] for c in self.colidxs]))

    def prepare(self, df):
        """Initialize the corresponding list of column indices for the columns
        that were specified at object instantiation. If the evaluation function
        is a prepared callable the respective prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        _, self.colidxs = select_clause(df=df, columns=self.columns)
        # Prepare the value function if it is not a prepared function.
        if not self.func.is_prepared():
            self.func.prepare(Sequence(df=df, columns=self.columns))
        return self


class SingleColumnEval(EvalFunction):
    """Eval function that evaluates a callable on a single column in a data
    frame row.
    """
    def __init__(self, func, columns):
        """Initialize the callable and the source column. Raises a ValueError
        if the function argument is not a callable.

        Parameters
        ----------
        func: callable
            Callable that is evaluated on a cell value from a data frame row.
        columns: int or string
            Single column index or name.

        Raises
        ------
        ValueError
        """
        # Raise ValueError if the function is not callable.
        if not callable(func):
            raise ValueError('not a callable')
        self.func = func
        self.columns = columns
        # The column index is initially None. The value will be initialized by
        # the prepare method.
        self.colidx = None

    def eval(self, values):
        """Evaluate the function on the cell value in the source column of the
        given data frame row.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        return self.func(values[self.colidx])

    def prepare(self, df):
        """Initialize the index of the source column in the schema of the given
        data frame. If the evaluation function is a prepared callable the
        respective prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        _, colidxs = select_clause(df=df, columns=self.columns)
        self.colidx = colidxs[0]
        # Prepare the callable if it is a prepared function.
        if isinstance(self.func, EvalFunction):
            self.func.prepare(df)
        return self


class SingleColumnValueEval(EvalFunction):
    """Eval function that evaluates a value function on a single column in a
    data frame row.
    """
    def __init__(self, func, columns):
        """Initialize the callable and the source column. Raises a ValueError
        if the function argument is not a callable.

        Parameters
        ----------
        func: (openclean.function.value.base.ValueFunction or callable)
            Callable or value function that is evaluated on a list of cell
            values from a data frame row.
        columns: int or string
            Single column index or name.

        Raises
        ------
        ValueError
        """
        # Ensure that the function is of type ValueFunction. Raises ValueError
        # if the function is not callable.
        if not isinstance(func, ValueFunction):
            func = CallableWrapper(func)
        self.func = func
        self.columns = columns
        # The column index is initially None. The value will be initialized by
        # the prepare method.
        self.colidx = None

    def eval(self, values):
        """Evaluate the function on the cell value in the source column of the
        given data frame row.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        return self.func(values[self.colidx])

    def prepare(self, df):
        """Initialize the index of the source column in the schema of the given
        data frame. If the evaluation function is a prepared callable the
        respective prepare() method is called.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        _, colidxs = select_clause(df=df, columns=[self.columns])
        self.colidx = colidxs[0]
        # Prepare the value function if it is not a prepared function.
        if not self.func.is_prepared():
            self.func.prepare(Sequence(df=df, columns=self.columns))
        return self


# -- Apply functions ----------------------------------------------------------

class Apply(object):
    """Apply a function on all values in specified column(s)."""
    def __new__(cls, columns, func):
        """Initialize the source column(s) and the applied function.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        func: callable
            Function that is applied to all values in the specified columns.
        """
        # Choose a pass-through function based on the columns argument and the
        # 'as_type' type cast argument.
        if is_var_func(columns):
            # -- Function that accepts a list of arguments
            func = VarApply(func)
        return Eval(func=func, columns=columns)


class ApplyFactory(metaclass=ABCMeta):
    """The apply factory is used to create instances of column functions for
    each column during the execution of the apply operator. Before the apply
    operator is executed the get_func() method of the factory is called for
    each function that apply() is executed on. The factory should return a
    column-specific callable. The returned callable will then be applied to
    each value in the respective column seperately.
    """
    @abstractmethod
    def get_func(self, df, colidx):
        """Get an instance of the column-specific apply function. The factory
        receives the data frame and the index of the column on whose values the
        returned function (callable) will be applied.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        colidx: int
            Index position of the column in the data frame.

        Returns
        -------
        callable
        """
        return NotImplementedError()


class VarApply(object):
    """Generic apply function for variable argument lists. Applies a given
    function on all arguments in a variable argument list.
    """
    def __init__(self, func):
        """Initialize the applied function.
        Parameters
        ----------
        func: callable
            Function that is applied to values in a variable argument list.
        """
        self.func = func

    def __call__(self, *args):
        """Apply function to each element in the argument list. Return list of
        modified values.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        list
        """
        return [self.func(arg) for arg in args]


# -- Helper functions ---------------------------------------------------------


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
