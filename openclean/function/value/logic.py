# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Basic logic operators as value function predicates."""

from openclean.function.value.base import CallableWrapper, ValueFunction


# -- Generic logic operator ---------------------------------------------------

class LogicFunction(ValueFunction):
    """Base class for logic functions. Each function can operate on a single
    predicate or a list of predicates.
    """
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition. Raises a
        ValueError if the elements in the predicate list are not value
        functions or callables.

        Parameters
        ----------
        args: callable, openclean.function.base.value.ValueFunction, or list
            Single callable or value function, or a list of callables or
            value functions.

        Raises
        ------
        ValueError
        """
        # Convert all elements in the list of predicates to evaluation
        # functions.
        self.predicates = list()
        for f in args:
            if isinstance(f, list):
                for el in f:
                    if not isinstance(el, ValueFunction):
                        if callable(el):
                            el = CallableWrapper(el)
                        else:
                            raise ValueError('not callable {}'.format(el))
                    self.predicates.append(el)
            else:
                if not isinstance(f, ValueFunction):
                    if callable(f):
                        f = CallableWrapper(f)
                    else:
                        raise ValueError('not callable {}'.format(f))
                self.predicates.append(f)

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
        """Returns False if any of the predicates requires preparation.
        Otherwise, no preparation is required.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if not f.is_prepared():
                return False
        return True

    def prepare(self, values):
        """The prepare step passes the values on to the predicates.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        if self.is_prepared():
            args = tuple([f.prepare(values) for f in self.predicates])
            return LogicFunction(*args)
        return self


# -- Operator instances for basic logic operations ----------------------------

class And(LogicFunction):
    """Logical conjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition. Raises a
        ValueError if the elements in the predicate list are not value
        functions or callables.

        Parameters
        ----------
        args: callable, openclean.function.base.value.ValueFunction, or list
            Single callable or value function, or a list of callables or
            value functions.

        Raises
        ------
        ValueError
        """
        super(And, self).__init__(*args)

    def eval(self, value):
        """Evaluate the predicates. Returns True if all predicates in the
        conjunction evaluate to True.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if not f.eval(value):
                return False
        return True


class Not(LogicFunction):
    """Logical negation of a predicate(s)."""
    def __init__(self, *args):
        """Initialize the predicate that is being negated. Raises a ValueError
        if more than one predicate is given.

        Parameters
        ----------
        args: callable, openclean.function.base.value.ValueFunction, or list
            Single callable or value function, or a list of callables or
            value functions.
        """
        super(Not, self).__init__(*args)
        # Ensure that the list of predicates only contains one element
        if len(self.predicates) != 1:
            raise ValueError('invalid arguments')

    def eval(self, value):
        """Return the negated result from evaluating the associated predicate
        on the given data frame row.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        bool
        """
        return not self.predicates[0].eval(value)


class Or(LogicFunction):
    """Logical disjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition.

        Parameters
        ----------
        args: callable, openclean.function.base.value.ValueFunction, or list
            Single callable or value function, or a list of callables or
            value functions.
        """
        super(Or, self).__init__(*args)

    def eval(self, value):
        """Evaluate the predicates. Returns True if at least one of the
        predicates in the disjunction evaluates to True.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if f.eval(value):
                return True
        return False
