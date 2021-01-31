# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
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
from typing import Dict, Iterable, List, Optional, Union

from openclean.data.types import Value


class Cluster(Counter):
    """Cluster of values. Maintains the frequency count for each value in order
    to be able to suggest a 'new values' as the common value for all values in
    the cluster.
    """
    def add(self, value: Value, count: Optional[int] = 1) -> Cluster:
        """Add a value to the cluster. Allows to provide a frequency count for
        the added value. Returns a reference to itself.

        Parameters
        ----------
        value: scalar or tuple
            Value that is added to the cluster.

        Returns
        -------
        openclean.cluster.base.Cluster
        """
        self[value] += count
        return self

    def suggestion(self) -> Value:
        """Suggest a new value as the common value for all values in the cluster.
        The suggestion is the most frequent value in the cluster. If multiple
        values have the same frequency the returned value depends on how ties
        are broken in the super class ``collections.Counter``.

        Returns
        -------
        scalar or tuple
        """
        return self.most_common(1)[0][0]

    def to_mapping(self, target: Optional[Value] = None) -> Dict:
        """Create a mapping for the values in the cluster to a given target
        value. This is primarily intended for standardization where all values
        in this cluster are mapped to a single target value.

        If the target value is not specified the suggested value for this
        cluster is used as the default.

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
        return {key: target for key in self.keys() if key != target}


class Clusterer(metaclass=ABCMeta):
    """The value clusterer mixin class defines a single method `clusters()` to
    cluster a given list of values.
    """
    @abstractmethod
    def clusters(self, values: Union[Iterable[Value], Counter]) -> List[Cluster]:
        """Compute clusters for a given list of values. Each cluster itself is
        a list of values, i.e., a subset of values from the input list. The
        cluster method should be capable of taking a list of values or a
        counter of distinct values.

        Parameters
        ----------
        values: iterable of values or collections.Counter
            Iterable of data values or a value counter that maps values to their
            frequencies.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        raise NotImplementedError()  # pragma: no cover


# -- Helper Classes -----------------------------------------------------------

class ONE(object):
    """Helper to simulate a counter where each value has a frequency of 1."""
    def __getitem__(self, key: Value) -> int:
        """All value lookups will return 1."""
        return 1
