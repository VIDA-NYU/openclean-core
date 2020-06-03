# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract bases classes for aggregate functions that are used by feature
profiling operators.
"""

from abc import ABCMeta, abstractmethod


class Aggregator(metaclass=ABCMeta):
    """Abstract base class for aggregation functions. Aggregators are used to
    compute an aggregated statistic over list of values.
    """
    def __init__(self, name):
        """Set the aggregator name that is used as the default label when the
        aggrgator is used in a profiling function.

        Parameters
        ----------
        name: string
            Aggregator name.
        """
        self.name = name

    @abstractmethod
    def exec(self, values):
        """Execute the aggregation function over a given list of scalar values
        or tuples of scalar values.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        scalar
        """
        raise NotImplementedError()

    def name(self):
        """Unique name for an aggragator. The name is used as the default label
        when the aggrgator is used in a profiling function.

        Returns
        -------
        string
        """
        return self.name
