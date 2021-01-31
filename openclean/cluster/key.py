# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Key-based clustering methods. Computes a different representation (key) for
each value and clusters values based on these keyes.
"""

from collections import Counter
from typing import Callable, Iterable, List, Optional, Tuple, Union

from openclean.data.types import Value
from openclean.cluster.base import Cluster, Clusterer, ONE
from openclean.engine.parallel import process_list
from openclean.function.value.key.fingerprint import Fingerprint
from openclean.function.value.base import CallableWrapper, ValueFunction

import openclean.config as config


class KeyCollisionCluster(Cluster):
    """Key collision clusters are used to represent results of the key collision
    clusterer. The key collision cluster extends the super class with a reference
    to the collision key for all values in the cluster.
    """
    def __init__(self, key: str):
        """Initialize the reference to the collision key.

        Parameters
        ----------
        key: string
            Collision key for all values in the cluster.
        """
        self.key = key


class KeyCollision(Clusterer):
    """Key collision methods create an alternative representation for each value
    (i.e., a  key), and then group values based on their keys.

    Generates clusters that satisfy a given minimum size threshold. Allows to
    compute keys in parallel using multiple threads.
    """
    def __init__(
        self, func: Union[Callable, ValueFunction], minsize: Optional[int] = 2,
        threads: Optional[int] = None
    ):
        """Initialize the key generator function, the minimal cluster size and
        the number of parallel threads.

        Parameters
        ----------
        func: callable or ValueFunction
            Function that is used to generate keys for values.
        minsize: int, default=2
            Minimum number of distinct values that each cluster in the returned
            result has to have.
        threads: int, default=None
            Number of parallel threads to use for key generation. If None the
            value from the environment variable 'OPENCLEAN_THREADS' is used as
            the default.
        """
        # Ensure that the function is a value function.
        self.func = CallableWrapper(func) if not isinstance(func, ValueFunction) else func
        self.minsize = minsize
        self.threads = threads if threads is not None else config.THREADS()

    def clusters(self, values: Union[Iterable[Value], Counter]) -> List[KeyCollisionCluster]:
        """Compute clusters for a given list of values. Each cluster itself is
        a list of values, i.e., a subset of values from the input list.

        Parameters
        ----------
        values: iterable of values or collections.Counter
            Iterable of data values or a value counter that maps values to their
            frequencies.

        Returns
        -------
        list of openclean.cluster.key.KeyCollisionCluster
        """
        # Return empty list if values are empty.
        if not values:
            return list()
        # Prepare the key generator if necessary.
        f = self.func if self.func.is_prepared() else self.func.prepare(values)
        # Create a list of key-value pairs for the values in the input list
        # using the key generator function.
        if self.threads > 1:
            kvps = process_list(
                func=KeyValueGenerator(f),
                values=values,
                processes=self.threads
            )
        else:
            kvps = [(f.eval(val), val) for val in values]
        # Sort key-value pairs by their key for clustering.
        kvps.sort(key=lambda t: t[0])
        # Create a frequency lookup function depending on whether we were given
        # a counter or simply a list of values.
        freq = values if isinstance(values, Counter) else ONE()
        clusters = list()
        # Get the key and value for the first element in the key-value pair
        # list. Create a first cluster for the value.
        cluster_key, val = kvps[0]
        cluster = KeyCollisionCluster(key=cluster_key).add(val, count=freq[val])
        # Iterate over the remaining values. Cluster values with equal key
        # values.
        for i in range(1, len(kvps)):
            key, val = kvps[i]
            if key == cluster_key:
                # Add value to the current cluster.
                cluster.add(val, count=freq[val])
            else:
                # Next key value detected. Add the current cluster to the result
                # if it contains at least minsize distinct values.
                if len(cluster) >= self.minsize:
                    clusters.append(cluster)
                # Create a new cluster for the next key value.
                cluster = KeyCollisionCluster(key=key).add(val, count=freq[val])
                cluster_key = key
        # Add the current cluster to the result if it contains at least minsize
        # distinct values.
        if len(cluster) >= self.minsize:
            clusters.append(cluster)
        return clusters


def key_collision(
    values: Union[Iterable[Value], Counter],
    func: Optional[Union[Callable, ValueFunction]] = None,
    minsize: Optional[int] = 2, threads: Optional[int] = None
) -> List[KeyCollisionCluster]:
    """Run key collision clustering for a given list of values.

    Parameters
    ----------
    values: iterable of values or collections.Counter
        Iterable of data values or a value counter that maps values to their
        frequencies.
    func: callable or ValueFunction, default=None
        Function that is used to generate keys for values. By default the
        token fingerprint generator is used.
    minsize: int, default=2
        Minimum number of distinct values that each cluster in the returned
        result has to have.
    threads: int, default=None
        Number of parallel threads to use for key generation. If None the
        value from the environment variable 'OPENCLEAN_THREADS' is used as
        the default.

    Returns
    -------
    list of openclean.cluster.key.KeyCollisionCluster
    """
    return KeyCollision(
        func=func if func is not None else Fingerprint(),
        minsize=minsize,
        threads=threads
    ).clusters(values=values)


# -- Helper classes -----------------------------------------------------------

class KeyValueGenerator(object):
    """Key-value pair generator for parallel processing."""
    def __init__(self, func: ValueFunction):
        """Initialize the key generator function.

        Parameters
        ----------
        func: ValueFunction
            Key generator function.
        """
        self.func = func

    def __call__(self, value: Value) -> Tuple[str, Value]:
        """Make the key generator callable. Return a key-value pair for a given
        value.

        Parameters
        ----------
        value: scalar or tuple
            Value for which a key is computed.

        Returns
        -------
        tuple of string and Value
        """
        return (self.func.eval(value), value)
