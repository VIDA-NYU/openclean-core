# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Interfaces for clustering. Openclean adopts the same notion of clustering
as OpenRefine: [...] clustering refers to the operation of 'finding groups of
different values that might be alternative representations of the same thing'*.

* https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Iterable, List, Tuple

from openclean.data.types import Value


class Cluster(object):
    """Cluster of values. Maintains the frequency count for each value in order
    to be able to suggest a 'new values' as the common value for all values in
    the cluster.
    """
    def __init__(self):
        """Initialize the dictionary that maps the cluster values to their
        identifier and frequency. Theidentifier defines the order in which
        values where inserted into the cluster.
        """
        self.elements = dict()

    def __iter__(self) -> Iterable[Tuple[Value, int]]:
        """Iterator over the distinct values in the cluster and their counts.

        Returns
        -------
        iterable
        """
        for key, value in self.elements.items():
            yield (key, value[1])

    def __len__(self) -> int:
        """Get number of distinct values in the cluster.

        Returns
        -------
        int
        """
        return len(self.elements)

    def __getitem__(self, key: int) -> Value:
        """Get the value that was inserted with the given identifier, i.e., the
        i-th value that was added to the cluster.

        Parameters
        ----------
        key: int
            Identifier for an inserted value.

        Returns
        -------
        scalar or tuple
        """
        for value, valmeta in self.elements.items():
            if valmeta[0] == key:
                return value
        raise KeyError('invalid key {}'.format(key))

    def add(self, value: Value) -> Cluster:
        """Add a value to the cluster. Returns a reference to itself.

        Parameters
        ----------
        value: scalar or tuple
            Value that is added to the cluster.

        Returns
        -------
        openclean.cluster.base.Cluster
        """
        if value in self.elements:
            insert_id, count = self.elements[value]
            self.elements[value] = (insert_id, count + 1)
        else:
            self.elements[value] = (len(self.elements), 1)
        return self

    def suggestion(self) -> Value:
        """Suggest a new value as the common value for all values in the cluster.
        The suggestion is the most frequent value in the cluster. If multiple
        values have the same frequency the order in which the values were added
        to the cluster is used as the tie-breaker, i.e., the value of those
        in the tie that was inserted first is returned as the result.

        Returns
        -------
        scalar or tuple
        """
        suggested_value = (None, -1, -1)
        for value, valmeta in self.elements.items():
            insert_id, count = valmeta
            max_count = suggested_value[2]
            if count > max_count:
                suggested_value = (value, insert_id, count)
            elif count == max_count and insert_id < suggested_value[1]:
                suggested_value = (value, insert_id, count)
        return suggested_value[0]


class Clusterer(metaclass=ABCMeta):
    """The value clusterer mixin class defines a single method `clusters()` to
    cluster a given list of values.
    """
    @abstractmethod
    def clusters(self, values: List[Value]) -> List[Cluster]:
        """Compute clusters for a given list of values. Each cluster itself is
        a list of values, i.e., a subset of values from the input list.

        Parameters
        ----------
        values: List of values
            List of data values.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        raise NotImplementedError()  # pragma: no cover
