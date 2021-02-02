# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Index structure for value clusters."""

from typing import List, Tuple

from openclean.cluster.base import Cluster


class Node(object):
    """Node in the cluster index."""
    def __init__(self, key: str, count: int):
        """Initialize the node key and frequency count.

        Parameters
        ----------
        key: string
            Value in a cluster.
        count: int
            Frequency of the value.
        """
        self.key = key
        self.count = count
        self.is_cluster = False
        self.children = None

    def add(self, values: List[Tuple[str, int]], pos: int) -> bool:
        """Add the values in the given list starting from ``pos`` to the
        children of this node.

        Returns True if at the end the cluster was added as a new cluster to
        the index.

        Parameters
        ----------
        values: list of tuples of string and count
            List of values and the frequencies in a cluster that is being
            added to the cluster index.
        pos: int
            Index position in the list that points to the child node that is
            added to this node.
        """
        # Ensure that the list of children is not None
        if self.children is None:
            self.children = dict()
        key, count = values[pos]
        if key not in self.children:
            self.children[key] = Node(key=key, count=count)
        node = self.children[key]
        if len(values) == pos + 1:
            # We reached the end of the list. Mark the node as an end node
            # pointing to a cluster in the generated tree.
            result = not node.is_cluster
            node.is_cluster = True
            return result
        else:
            return node.add(values, pos + 1)


class ClusterIndex(object):
    """Index structure to maintain a set of clusters. Implements a prefix
    tree.
    """
    def __init__(self):
        """Initialize the root of the tree."""
        self.root = Node(key=None, count=0)

    def add(self, cluster: Cluster) -> bool:
        """Add the given cluster to the index. Returns True if the cluster was
        added as a new cluster (i.e., it did not exist in the index before) and
        False otherwise.

        Parameters
        ----------
        cluster: openclean.cluser.base.Cluster
            Cluster of data value.

        Returns
        -------
        bool
        """
        # Sort cluster values in natural order.
        values = sorted(cluster.items(), key=lambda x: x[0])
        return self.root.add(values, 0)
