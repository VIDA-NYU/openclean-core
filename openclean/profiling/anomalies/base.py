# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for anomaly and outlier detection operators."""

from abc import ABCMeta, abstractmethod

from openclean.function.value.base import ProfilingFunction


class AnomalyDetector(ProfilingFunction, metaclass=ABCMeta):
    """Interface for generic anomaly and outlier detectors. Each implementation
    should take a stream of distinct values (e.g., from a column in a data
    frame or a metadata object) as input and return a list of values that were
    identified as outliers.
    """
    def __init__(self, name):
        """Set the function name.

        Parameters
        ----------
        name: string
            Unique function name.
        """
        super(AnomalyDetector, self).__init__(name=name)
        
    @abstractmethod
    def find(self, values):
        """Identify values in a given stream of (distinct) values that are
        classified as outliers or anomalities. Returns a list of identified
        values.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        raise NotImplementedError()
