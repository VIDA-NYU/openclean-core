# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for unique column combination (UCC) discovery. UCCs are a
prerequisite for unique constraints and keys.
"""

from abc import ABCMeta, abstractmethod
from typing import List, Union

import pandas as pd

from openclean.data.column import Column


class UniqueColumnSet(set):
    """Unique column combinations are lists of column names of identifiable
    column objects.

    For now, this is a simple wrapper around a Python set. In the future we may
    want to add additional functionality to maintain metadata for a column
    combination.
    """
    def __init__(
        self, columns: List[Union[str, Column]] = None,
        duplicate_ok: bool = True
    ):
        """Initialize the set of unique columns. Raises a ValueError if the
        elements in the list are not unique and the duplicate_ok flag is False.

        Parameters
        ----------
        columns: list, default=None
            List of column names or identifiable columns.
        duplicate_ok: bool, default=True
            Raise a ValueError if the flag is True and the given column list
            contains duplicate entries.
        """
        if columns is not None:
            for col in columns:
                if col in self and not duplicate_ok:
                    raise ValueError('duplicate column {}'.format(col))
                self.add(col)


class UniqueColumnCombinationFinder(metaclass=ABCMeta):
    """Interface for operators that discover combinations of unique columns in
    a given data frame.
    """
    @abstractmethod
    def run(self, df: pd.DataFrame) -> List[UniqueColumnSet]:
        """Run the implemented unique column combination discovery algorithm on
        the given data frame. Returns a list of all discovered unique column
        sets.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        list of UniqueColumnSet
        """
        raise NotImplementedError()  # pragma: noqa
