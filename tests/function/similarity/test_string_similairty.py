# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for string similarity functions."""

import jellyfish
import pytest

import openclean.function.similarity.text as ssim

V1 = 'BROOKLYN'
V2 = 'BOROKLYMN'


@pytest.mark.parametrize(
    'func,sim',
    [
        (ssim.LevenshteinDistance(), 1 - (3 / 9)),
        (ssim.DamerauLevenshteinDistance(), 1 - (2 / 9)),
        (ssim.HammingDistance(), 1 - (4 / 9)),
        (ssim.JaroSimilarity(), jellyfish.jaro_similarity(V1, V2)),
        (ssim.JaroWinklerSimilarity(), jellyfish.jaro_winkler_similarity(V1, V2)),
        (ssim.MatchRatingComparison(), 1)
    ]
)
def test_compare_two_strings(func, sim):
    assert func(V1, V2) == sim
