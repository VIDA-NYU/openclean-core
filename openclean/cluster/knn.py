# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of the Nearest Neighbor clustering (also known as kNN) that
use a string similarity function and a threshold (radius).

The kNN clustering brings together strings that have a similarity which is
within the given radius constraint. This implementation is based on the hybrid
blocking approach that is implemented in OpenRefine:
https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth

The algorithm works by performing a first pass over the strings in order to
group them into blocks of strings that share at least on n-gram. It then uses
a given string similarity function to compute similarity between strings in the
created blocks.
"""

from collections import Counter, defaultdict
from typing import Iterable, List, Optional, Union

from openclean.data.types import Value
from openclean.cluster.base import Cluster, Clusterer, ONE
from openclean.function.token.base import StringTokenizer
from openclean.function.token.ngram import NGrams
from openclean.function.similarity.base import SimilarityConstraint


class kNNClusterer(Clusterer):
    """Nearest Neighbor clustering algorithm that is based on a hybrid
    clustring approach.

    The algorithm works by performing a first pass over the strings in order to
    group them into blocks of strings that share at least on token (e.g.,
    n-gram). It then uses a given string similarity function to compute
    similarity between strings in the created blocks.
    """
    def __init__(
        self, sim: SimilarityConstraint,
        tokenizer: Optional[StringTokenizer] = None, minsize: Optional[int] = 2,
        remove_duplicates: Optional[bool] = True
    ):
        """Initialize the string tokenizer, the similarity constraint, and the
        minimal size for generated clusters.

        Parameters
        ----------
        sim: openclean.function.similarity.base.SimilarityConstraint
            String similarity constraint for grouping strings in the generated
            blocks.
        tokenizer: openclean.function.token.base.StringTokenizer, default=None
            Generator for tokens that are used to group string values in the
            first step of the algorithm. By default, n-grams of length 6 are
            used as blocking tokens.
        minsize: int, default=2
            Minimum number of distinct values that each cluster in the returned
            result has to have.
        remove_duplicates: bool, default=True
            Remove identical clusters from the result if True.
        """
        self.sim = sim
        self.tokenizer = tokenizer if tokenizer else NGrams(n=6)
        self.minsize = minsize
        self.remove_duplicates = remove_duplicates

    def clusters(self, values: Union[Iterable[Value], Counter]) -> List[Cluster]:
        """Compute clusters for a given list of values. Each cluster itself is
        a list of values, i.e., a subset of values from the input list.

        Parameters
        ----------
        values: iterable of values or collections.Counter
            Iterable of data values or a value counter that maps values to their
            frequencies.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        # Return empty list if values are empty.
        if not values:
            return list()
        # Create blocks of values based on tokens generated by the given
        # tokenizer.
        blocks = self._get_blocks(values)
        # Create a frequency lookup function depending on whether we were given
        # a counter or simply a list of values.
        freq = values if isinstance(values, Counter) else ONE()
        # Group values within blocks based on string similarity.
        clusters = defaultdict(Cluster)
        for block in blocks:
            # Ignore blocks with only one element.
            if len(block) < 2:
                continue
            for i in range(len(block) - 1):
                val_i = block[i]
                for j in range(i + 1, len(block)):
                    val_j = block[j]
                    # No need to compare if values are already part of each
                    # others neighbors (only need to check for one value since
                    # this is a symetric operation).
                    if val_j in clusters.get(val_i, dict()):
                        continue
                    # Do nothing if the two values do not satisfy the similarity
                    # constraint.
                    if not self.sim.is_satisfied(val_i, val_j):
                        continue
                    # Add values to their respective neighbor sets.
                    clusters[val_i].add(val_j, freq[val_j])
                    clusters[val_j].add(val_i, freq[val_i])
        # Add each value to it's own cluster.
        for key in clusters.keys():
            clusters[key].add(key, freq[key])
        # Return clusters that satisfy the minimum size constraint. Remove
        # duplicates if the respective flag is True.
        return self._get_clusters(clusters.values())

    def _get_blocks(self, values) -> Iterable[List]:
        """Get blocks of values based on common tokens.

        Parameters
        ----------
        values: iterable of values or collections.Counter
            Iterable of data values or a value counter that maps values to their
            frequencies.

        Returns
        -------
        iterable list
        """
        blocks = defaultdict(list)
        for value in values:
            for key in set(self.tokenizer.tokens(value)):
                blocks[key].append(value)
        return blocks.values()

    def _get_clusters(self, clusters: Iterable[Cluster]) -> List[Cluster]:
        """Filter clusters from a list of candidates.

        Removes clusters that do not satisfy the minimum size constraint.
        Removes duplicates if the respective flag is True.

        Parameters
        ----------
        clusters: iterable of openclean.cluster.base.Cluster
            Candidate clusters.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        result = list()
        for cluster in clusters:
            is_duplicate = False
            if self.remove_duplicates:
                for c in result:
                    if c == cluster:
                        is_duplicate = True
                        break
            if not is_duplicate:
                result.append(cluster)
        return result


def knn_clusters(
    values: Union[Iterable[Value], Counter], sim: SimilarityConstraint,
    tokenizer: Optional[StringTokenizer] = None, minsize: Optional[int] = 2,
    remove_duplicates: Optional[bool] = True
) -> List[Cluster]:
    """Run kNN clustering for a given list of values.

    Parameters
    ----------
    values: iterable of values or collections.Counter
        Iterable of data values or a value counter that maps values to their
        frequencies.
    sim: openclean.function.similarity.base.SimilarityConstraint
        String similarity constraint for grouping strings in the generated
        blocks.
    tokenizer: openclean.function.token.base.StringTokenizer, default=None
        Generator for tokens that are used to group string values in the
        first step of the algorithm. By default, n-grams of length 6 are
        used as blocking tokens.
    minsize: int, default=2
        Minimum number of distinct values that each cluster in the returned
        result has to have.
    remove_duplicates: bool, default=True
        Remove identical clusters from the result if True.

    Returns
    -------
    list of openclean.cluster.base.Cluster
    """
    return kNNClusterer(
        sim=sim,
        tokenizer=tokenizer,
        minsize=minsize,
        remove_duplicates=remove_duplicates
    ).clusters(values=values)
