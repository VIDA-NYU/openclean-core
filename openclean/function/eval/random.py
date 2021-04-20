# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Eval function for random number generator."""

from random import Random
from typing import List, Optional

import pandas as pd

from openclean.data.stream.base import DataRow, StreamFunction
from openclean.data.types import DatasetSchema, Value
from openclean.function.eval.base import EvalFunction


# -- Domain membership predicates ---------------------------------------------

class Rand(EvalFunction):
    """Evaluation function that returns a random number in the interval [0, 1).
    This function can for example be used to randomly select rows in a data
    frame using a probability threshold.
    """
    def __init__(self, seed: Optional[int] = None):
        """Initialize the random number generator.

        Parameters
        ----------
        seed: int, default=None
            Seed value for the random number generator (for reproducibility
            purposes).
        """
        self.rand = Random()
        if seed is not None:
            self.rand.seed(seed)

    def __call__(self, row: DataRow) -> Value:
        """Make the object a stream function. Returns a random number in the
        interval [0, 1) for every row that the function is called for.

        Parameters
        ----------
        row: list of scalar
            Row in a data stream.

        Returns
        -------
        float
        """
        return self.rand.random()

    def eval(self, df: pd.DataFrame) -> List[Value]:
        """Return a list of random numbers in the interval [0, 1).

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        list
        """
        return [self.rand.random() for i in range(df.shape[0])]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
        """The prepare method returns a callable that returns a random number
        for evary input row.

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        return self
