# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for anomaly and outlier detection operators."""

from collections import Counter
from typing import List

from openclean.data.types import Value
from openclean.profiling.base import DistinctSetProfiler


class AnomalyDetector(DistinctSetProfiler):
    """Interface for generic anomaly and outlier detectors. Each implementation
    should take a stream of distinct values (e.g., from a column in a data
    frame or a metadata object) as input and return a list of values that were
    identified as outliers.
    """
    def find(self, values: List[Value]) -> List[Value]:
        """Identify values in a given set of values that are classified as
        outliers or anomalities. Returns a list of identified values.

        Parameters
        ----------
        values: list
            List of input values.

        Returns
        -------
        list
        """
        return list(self.process(Counter(values)).keys())
