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
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple, Union

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

    def add(self, value: Value, count: Optional[int] = 1) -> Cluster:
        """Add a value to the cluster. Allows to provide frequency count for
        the added value. Returns a reference to itself.

        Parameters
        ----------
        value: scalar or tuple
            Value that is added to the cluster.

        Returns
        -------
        openclean.cluster.base.Cluster
        """
        if value in self.elements:
            insert_id, vcount = self.elements[value]
            self.elements[value] = (insert_id, vcount + count)
        else:
            self.elements[value] = (len(self.elements), count)
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

    def to_mapping(self, target: Optional[Value] = None) -> Dict:
        """Create a mapping for the values in the cluster to a given target
        value. This is primarily intended for standardization where all values
        matchin values in this cluster are mapped to a single target value.

        If the target value is not specified the suggested value for this cluster
        is used as the default.

        The resulting mapping will not include an entry for the target itself.
        That is, if the target is a value in the cluster that entry is excluded
        from the generated mapping.

        Parameters
        ----------
        target: scalar or tuple, default=None
            Target value to which all values in this cluster are mapped.

        Returns
        -------
        dict
        """
        target = target if target is not None else self.suggestion()
        return {key: target for key in self.elements if key != target}


class Clusterer(metaclass=ABCMeta):
    """The value clusterer mixin class defines a single method `clusters()` to
    cluster a given list of values.
    """
    @abstractmethod
    def clusters(self, values: Union[List[Value], Counter]) -> List[Cluster]:
        """Compute clusters for a given list of values. Each cluster itself is
        a list of values, i.e., a subset of values from the input list. The
        cluster method should be capable of taking a list of values or a
        counter of distinct values.

        Parameters
        ----------
        values: List of values or collections.Counter
            List of data values or a value counter that maps values to their
            frequencies.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        raise NotImplementedError()  # pragma: no cover
