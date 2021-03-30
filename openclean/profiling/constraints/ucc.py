# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for unique column combination (UCC) discovery. UCCs are a
prerequisite for unique constraints and keys.
"""

from abc import ABCMeta, abstractmethod
from typing import List

import pandas as pd

from openclean.data.types import Columns


class UniqueColumnCombinationFinder(metaclass=ABCMeta):
    """Interface for operators that discover combinations of unique columns in
    a given data frame.
    """
    @abstractmethod
    def run(self, df: pd.DataFrame) -> List[Columns]:
        """Run the implemented unique column combination discovery algorithm on
        the given data frame. Returns a list of all discovered unique column
        sets.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        list
        """
        raise NotImplementedError()  # pragma: no cover
