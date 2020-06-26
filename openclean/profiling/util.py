# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper functions for data profilers."""


def always_false(*args):
    """Predicate that always evaluates to False.

    Parameters
    ----------
    args: any
        Variable list of arguments.

    Returns
    -------
    bool
    """
    return False


def always_true(*args):
    """Predicate that always evaluates to True.

    Parameters
    ----------
    args: any
        Variable list of arguments.

    Returns
    -------
    bool
    """
    return True


class eval_all(object):
    """Logic operator that evaluates a list of predicates and returns True only
    if all predicates return a defined result value.
    """
    def __init__(self, predicates, truth_value=True):
        """Initialize the list of predicates and the expected result value.

        Parameters
        ----------
        predicates: list(callable)
            List of callables that are evaluated on a given value.
        truth_value: scalar, default=True
            Expected result value for predicate evaluation to be considered
            satisfied.
        """
        self.predicates = predicates
        self.truth_value = truth_value

    def __call__(self, value):
        """Evaluate all predicates on the given value. Returns True only if all
        predicates evaluate to the defined result value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if f.eval(value) != self.truth_value:
                return False
        return True


def get_threshold(threshold):
    """Ensure that the given threshold is a callable.

    Parameters
    ----------
    threshold: callable, int or float
        Expects a callable or a numeric value that will be wrapped in a
        comparison operator.

    Retuns
    ------
    callable

    Raise
    -----
    ValueError
    """
    # If the given threshold is None return a function that always returns
    # True, i.e., any value satisfies the threshold.
    if threshold is None:
        return always_true
    # If the threshold is an integer or float create a greater than threshold
    # using the value (unless the value is 1 in which case we use eq).
    if type(threshold) in [int, float]:
        if threshold == 1:

            def is_one(value):
                return value == 1

            threshold = is_one
        else:

            def gt(value):
                return value > threshold

            threshold = gt
    elif not callable(threshold):
        raise ValueError('invalid threshold constraint')
    return threshold
