# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for functional dependency (FD) discovery. FDs express
relationships between attributes of a dataset. FDs were originally used in
database design, especially schema normalization. FDs can also be used for
data cleaning purposes to identfy sets of rows (tuples) that violate a given
constraint and are therefore candidates for data repair.
"""

from abc import ABCMeta, abstractmethod
from typing import List

import pandas as pd

from openclean.data.types import Columns


class FunctionalDependency(object):
    """Functional dependencies describe a relationship between two sets of
    attributes. These sets are referred to as the determinant (left-hand-size)
    and dependant (right-hand-size).

    """
    def __init__(self, lhs: Columns, rhs: Columns):
        """Initialize the determinant (left-hand-size) and dependant
        (right-hand-size) of the functional dependency.

        Parameters
        ----------
        lhs: int, string, or list of int or string of Column
            Left-hand-side of the functional dependency (determinant).
        rhs: int, string, or listof int or string
            Right-hand-side of the functional dependency (dependant).
        """
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        """String representation for the functional dependency."""
        return '[{}] -> [{}]'.format(','.join(self.lhs), ','.join(self.rhs))

    @property
    def dependant(self) -> Columns:
        """Get the dependant (right-hand-side) of the functional dependency.

        Returns
        -------
        int, string, or list of int or string of Column
        """
        return self.rhs

    @property
    def determinant(self) -> Columns:
        """Get the determinant (left-hand-side) of the functional dependency.

        Returns
        -------
        int, string, or list of int or string of Column
        """
        return self.lhs


class FunctionalDependencyFinder(metaclass=ABCMeta):
    """Interface for operators that discover functional dependencies in a given
    data frame.
    """
    @abstractmethod
    def run(self, df: pd.DataFrame) -> List[FunctionalDependency]:
        """Run the implemented functional dependency discovery algorithm on
        the given data frame. Returns a list of all discovered functional
        dependencies.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        list of FunctionalDependency
        """
        raise NotImplementedError()  # pragma: no cover
