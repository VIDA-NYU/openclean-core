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


# -- Generic logic operator ---------------------------------------------------

class LogicOperator(EvalFunction):
    """Base class for logic operators."""
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition. Raises a
        ValueError if the elements in the predicate list are not evaluation
        functions or callables.

        Parameters
        ----------
        predicates: callable, openclean.function.eval.base.EvalFunction, or
                list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.

        Raises
        ------
        ValueError
        """
        # Convert all elements in the list of predicates to evaluation
        # functions.
        self.predicates = list()
        for f in args:
            if not isinstance(f, EvalFunction):
                if callable(f):
                    f = FullRowEval(func=f)
                else:
                    raise ValueError('not a callable {}'.format(f))
            self.predicates.append(f)

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
        # Prepare the evaluation functions.
        for f in self.predicates:
            f.prepare(df)
        return self


# -- Operator instances for basic logic operations ----------------------------

class And(LogicOperator):
    """Logical conjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition. Raises a
        ValueError if the elements in the predicate list are not evaluation
        functions or callables.

        Parameters
        ----------
        args: callable, openclean.function.eval.base.EvalFunction, or list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.

        Raises
        ------
        ValueError
        """
        super(And, self).__init__(*args)

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


class Not(LogicOperator):
    """Logical negation of a predicate(s)."""
    def __init__(self, *args):
        """Initialize the predicate that is being negated. Raises a ValueError
        if more than one predicate is given.

        Parameters
        ----------
        args: callable, openclean.function.eval.base.EvalFunction, or list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.
        """
        super(Not, self).__init__(*args)
        # Ensure that the list of predicates only contains one element
        if len(self.predicates) != 1:
            raise ValueError('invalid arguments')

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
        return not self.predicates[0](values)


class Or(LogicOperator):
    """Logical disjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the proposition.

        Parameters
        ----------
        args: callable, openclean.function.eval.base.EvalFunction, or list
            Single callable or evaluation function, or a list of callables or
            evaluation functions.
        """
        super(Or, self).__init__(*args)

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
