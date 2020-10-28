# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for using the fuzzy matcher to update misspellings in a data
frame column.
"""

from openclean.function.matching.fuzzy import FuzzySimilarity
from openclean.function.matching.base import DefaultStringMatcher
from openclean.function.matching.mapping import best_matches
from openclean.function.value.domain import BestMatch
from openclean.operator.transform.update import update


def test_update_bestmatch_column(nyc311):
    """Test updating values in a single column with their best match mappings
    of a data frame.
    """
    domain_pronounciation = [
        'bronks',
        'brooklen',
        'manhatn',
        'kweenz',
        'staten islen'
    ]

    vocabulary = DefaultStringMatcher(
        vocabulary=domain_pronounciation,
        similarity=FuzzySimilarity(),
        no_match_threshold=0.
    )

    # replace with the best match in the given vocabulary
    df = update(nyc311, 'borough', BestMatch(matcher=vocabulary))
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(domain_pronounciation)

    # Manually create a mapping to let user manipulate it before updating the
    # data frame.
    map = best_matches(values=nyc311['borough'].unique(), matcher=vocabulary)
    # override best match from the vocabulary
    map.update({'STATEN ISLAND': 'statn ayeln'})

    df = update(nyc311, 'borough', map.to_lookup())
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(map.to_lookup().values())
