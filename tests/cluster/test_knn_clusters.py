# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the kNN cluster method."""

from openclean.cluster.knn import knn_clusters, knn_collision_clusters
from openclean.function.similarity.base import SimilarityConstraint
from openclean.function.similarity.text import LevenshteinDistance
from openclean.function.token.ngram import NGrams
from openclean.function.value.threshold import GreaterThan


VALUES = [
    'BROOKLYN',
    'BRPPKLYN',
    'BROKLYN',
    'BROPKLYN',
    'QUEENS',
    'QUEEENS',
    'QUEENZ',
    'MANHATTAN',
    'MANNHATAN',
    'MANNHATTAN',
    'BRONX',
    'BRONZ'
]


def test_empty_knn_clusters():
    """Test the kNN clustering with an empty input list."""
    clusters = knn_clusters(
        values=[],
        sim=SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0)),
    )
    assert len(clusters) == 0


def test_knn_clusters():
    """Test the kNN clustering method on a set of misspelled borough names."""
    clusters = knn_clusters(
        values=VALUES,
        sim=SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.7)),
        tokenizer=NGrams(n=4),
        minsize=2
    )
    assert len(clusters) == 4
    clusters = knn_clusters(
        values=VALUES,
        sim=SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.7)),
        tokenizer=NGrams(n=4),
        minsize=3
    )
    assert len(clusters) == 3
    clusters = knn_clusters(
        values=VALUES,
        sim=SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.7)),
        tokenizer=NGrams(n=4),
        minsize=2,
        remove_duplicates=False
    )
    assert len(clusters) == 12
    clusters = knn_clusters(
        values=VALUES,
        sim=SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.97)),
        tokenizer=NGrams(n=4),
        minsize=2
    )
    assert len(clusters) == 0


def test_knn_collision_clusters():
    """Test the kNN clustering method that performs key collision clustering
    as a pre-processing step.
    """
    values = ['AMY/S PIZZA/S.', 'AMY\'S PIZZA', 'AMMYS PIZZA']
    sim = SimilarityConstraint(func=LevenshteinDistance(), pred=GreaterThan(0.9))
    clusters = knn_clusters(
        values=values,
        sim=sim,
        tokenizer=NGrams(n=4),
        minsize=2
    )
    assert len(clusters) == 0
    clusters = knn_collision_clusters(
        values=values,
        sim=sim,
        tokenizer=NGrams(n=4),
        minsize=2
    )
    assert len(clusters) == 1
