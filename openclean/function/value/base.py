# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base class for value function. Collection of basic helper functions."""

from abc import ABCMeta, abstractmethod


# -- Abstract base class for value functions ----------------------------------

class ListFunction(metaclass=ABCMeta):
    """Interface for functions that transform a given list of values."""
    @abstractmethod
    def apply(self, values):
        """Apply a function to each value in a given list. Returns a list of
        values that are the result of evaluating an assicuated value function
        for the respective input values.

        Should call the prepare method of an associated value function before
        executing the eval method on each individual value in the given list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        raise NotImplementedError()


class ValueFunction(ListFunction, metaclass=ABCMeta):
    """The abstract class for value functions defines the interface for methods
    that need to be implemented for preparing and evaluating the function.
    """
    def apply(self, values):
        """Apply the function to each value in a given list. Returns a list of
        values that are the result of the eval method for the respective input
        values.

        Calls the prepare method before executing the eval method on each
        individual value in the given list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        f = self.prepare(values)
        return [f.eval(v) for v in values]

    @abstractmethod
    def eval(self, value):
        """Evaluate the function on a given value. The value may either be a
        scalar or a tuple. The value will be from the list of values that was
        passed to the object in the prepare call.

        The return value of the function is implementation dependent.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        raise NotImplementedError()

    @abstractmethod
    def is_prepared(self):
        """Returns True if the prepare method is ignored by an implementation
        of this function. Containing classes will only call the prepare method
        for those value functions that are not prepared.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def prepare(self, values):
        """Optional step to prepare the function for a given list of values.
        This step allows to compute statistics over the list of values for
        which the eval method will be called.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ValueFunction
        """
        raise NotImplementedError()


# -- Default base class implementations ---------------------------------------

class PreparedFunction(ValueFunction):
    """Abstract base class for value functions that do not make use of the
    prepare method. These functions are considered as initialized and ready
    to operate without the need for calling the prepare method first.
    """
    def __call__(self, value):
        """Make the function callable for individual values.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        return self.eval(value)

    def is_prepared(self):
        """Instances of this class do not need to be further prepared.

        Returns
        -------
        bool
        """
        return True

    def prepare(self, values):
        """The prepare step is ignored for a wrapped callable.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        return self


class CallableWrapper(PreparedFunction):
    """Wrapper for callable functions as value functions. This value function
    does not prepare the wrapped callable.
    """
    def __init__(self, func):
        """Initialize the wrapped callable function. Raises a ValueError if the
        function is not a callable.

        Parameters
        ----------
        func: callable
            Function that is wrapped as a value finction.

        Raises
        ------
        ValueError
        """
        # Ensure that the given function is actually a callable.
        if not callable(func):
            raise ValueError('not a callable function')
        self.func = func

    def eval(self, value):
        """Evaluate the wrapped function on a given value. The value may either be a
        scalar or a tuple. The return value of the function is dependent on the
        wrapped function.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        return self.func(value)


class ConstantValue(PreparedFunction):
    """Value function that always returns a given constant value."""
    def __init__(self, value):
        """Initialize the constant value that is returned by this function
        whenever the eval method is called..

        Parameters
        ----------
        value: scalar or tuple
            Constant return value for the eval method.
        """
        self.const = value

    def eval(self, value):
        """Evaluate the wrapped function on a given value. The value may either be a
        scalar or a tuple. The return value of the function is dependent on the
        wrapped function.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        return self.const


# -- Helper functions ---------------------------------------------------------

def scalar_pass_through(value):
    """Pass-through method for single scalar values.

    Parameters
    ----------
    value: scalar
        Scalar cell value from a data frame row.

    Returns
    -------
    scalar
    """
    return value


def to_valuefunc(value):
    """Return a value function that represents the given argument. If the
    argument is not a value function, either of the following is expected:
    (i) a callable that will be wrapped in a CallableWrapper, or (2) a constant
    value that will be wrapped in a ConstantValue.

    Parameters
    ----------
    value: scalar, callable, or openclean.function.value.base.ValueFunction
        Argument that is being represented as a value function.

    Returns
    -------
    openclean.function.value.base.ValueFunction
    """
    if not isinstance(value, ValueFunction):
        if callable(value):
            value = CallableWrapper(value)
        else:
            value = ConstantValue(value)
    return value
