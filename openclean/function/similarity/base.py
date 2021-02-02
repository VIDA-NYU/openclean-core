# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for similarity functions and similarity constraints."""

from abc import ABCMeta, abstractmethod
from typing import Callable

from openclean.data.types import Value


class SimilarityFunction(metaclass=ABCMeta):
    """Mixin class for functions that compute the similarity between two
    values (scalar or tuples). Primarily useful for string similarity.

    Similarity results are float values in the interval [0-1] where 0 is the
    minimal similarity between two values and 1 is the maximal similarity.
    """
    def __call__(self, val_1: Value, val_2: Value) -> float:
        """Make the similarity function callable.
        """
        return self.sim(val_1=val_1, val_2=val_2)

    @abstractmethod
    def sim(self, val_1: Value, val_2: Value) -> float:
        """Compute similarity between between two values.

        The result is in the interval [0-1] where 0 is the minimal similarity
        between two values and 1 is the maximal similarity.

        Parameters
        ----------
        val_1: scalar or tuple
        val_2: scalar or tuple

        Returns
        -------
        float
        """
        raise NotImplementedError()  # pragma: no cover


class SimilarityConstraint(object):
    """Function that validates a constraint, e.g., a threshold predicate, on
    the similarity between two values (scalar or tuples).

    This class is a simple wrapper around a similarity function and a predicate
    that is evaluated on the similarity score for a given pair of values.
    """
    def __init__(self, func: SimilarityFunction, pred: Callable):
        """Initialize the similarity function and the predicate representing a
        constraint.

        The predicate a callabel that expects a single argument. It is not a
        :class:ValueFunction since the similarity constraint does not provide
        the option to prepare the value function if needed.

        Parameters
        ----------
        func: openclean.function.similairty.base.SimilarityFunction
            Function to compute the similarity between a pair of values.
        pred: callable
            Boolean function that is called with the similarity score for a
            pair of values.
        """
        self.func = func
        self.pred = pred

    def __call__(self, val_1: Value, val_2: Value) -> bool:
        """Make the function callable.

        Parameters
        ----------
        val_1: scalar or tuple
        val_2: scalar or tuple

        Returns
        -------
        bool
        """
        return self.is_satisfied(val_1=val_1, val_2=val_2)

    def is_satisfied(self, val_1: Value, val_2: Value) -> bool:
        """Test if a given pair of values satisfies the similarity constraint.

        Returns True if the similarity between ``val_1``` and ``val_2``
        satisfies the constraint (e.g., a given trheshold).

        Parameters
        ----------
        val_1: scalar or tuple
        val_2: scalar or tuple

        Returns
        -------
        bool
        """
        return self.pred(self.func(val_1, val_2))
