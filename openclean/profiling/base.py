# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for operators and functions that operate on a stream of
data values.
"""

from abc import ABCMeta, abstractmethod


class DatastreamProfiler(metaclass=ABCMeta):
    """Profiler for a stream of data values. Values are either scalar values
    or tuples of scalar values. Each stream profiler implements the apply()
    method that consumes an iterable stream of values. The profiling result
    is a dictionary. The keys and values in the dictionary are implementation
    dependent and differ for each profiler.
    """
    @abstractmethod
    def apply(self, stream):
        """Compute statistic over all the values in a given data stream.

        Parameters
        ----------
        stream: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        raise NotImplementedError()
