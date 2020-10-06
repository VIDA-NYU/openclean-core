# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of basic logic operators as evaluation function predicates
for data frame rows.
"""

from openclean.function.eval.base import EvalFunction, FullRowEval


class And(EvalFunction):
    """Logical conjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the conjunction.

        Parameters
        ----------
        args: callable, openclean.function.eval.base.EvalFunction, or
                list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.
        """
        # Convert all elements in the list of predicates to evaluation
        # functions.
        self.predicates = list()
        for f in args:
            self.predicates.append(to_eval(f))

    def eval(self, values):
        """Evaluate the predicates. Returns True if all predicates in the
        conjunction evaluate to True.

        Parameters
        ----------
        values: pandas.core.series.Series
            Pandas data frame row object.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if not f.eval(values):
                return False
        return True

    def prepare(self, df):
        """Call the respective prepare() method for the individual predicates.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        return And(*[f.prepare(df) for f in self.predicates])


class Not(EvalFunction):
    """Logical negation of a predicate(s)."""
    def __init__(self, predicate):
        """Initialize the predicate that is being negated. Raises a ValueError
        if more than one predicate is given.

        Parameters
        ----------
        predicate: callable or openclean.function.eval.base.EvalFunction
            Single callable or evaluation function.
        """
        self.predicate = to_eval(predicate)

    def eval(self, values):
        """Return the negated result from evaluating the associated predicate
        on the given data frame row.

        Parameters
        ----------
        values: pandas.core.series.Series
            Pandas data frame row object

        Returns
        -------
        bool
        """
        return not self.predicate.eval(values)

    def prepare(self, df):
        """Call the respective prepare() method for the predicate.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        return Not(self.predicate.prepare(df))


class Or(EvalFunction):
    """Logical disjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the disjunction.

        Parameters
        ----------
        args: callable, openclean.function.eval.base.EvalFunction, or
                list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.
        """
        # Convert all elements in the list of predicates to evaluation
        # functions.
        self.predicates = list()
        for f in args:
            self.predicates.append(to_eval(f))

    def eval(self, values):
        """Evaluate the predicates. Returns True if at least one of the
        predicates in the disjunction evaluates to True.

        Parameters
        ----------
        values: pandas.core.series.Series
            Pandas data frame row object

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if f.eval(values):
                return True
        return False

    def prepare(self, df):
        """Call the respective prepare() method for the individual predicates.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        return Or(*[f.prepare(df) for f in self.predicates])


# -- Helper Methods -----------------------------------------------------------

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
        return FullRowEval(func=value)
    return value
