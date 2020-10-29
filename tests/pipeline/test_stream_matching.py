# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the match operator in data processing pipelines."""

import pandas as pd
import pytest


from openclean.function.matching.base import DefaultStringMatcher
from openclean.function.value.phonetic import Soundex
from openclean.pipeline import stream


@pytest.fixture
def dataset():
    """Get dataset with columns 'city' and 'X'."""
    return pd.DataFrame(
        data=[['Brooklyn', 0], ['Brooklin', 1], ['Queens', 2], ['Quens', 3]],
        columns=['city', 'X']
    )


def test_match_vacabulary_in_stream(dataset):
    """Test matching values in a data stream against a given vocabulary."""
    vocab = DefaultStringMatcher(
        vocabulary=['Brooklyn', 'Queens'],
        similarity=Soundex()
    )
    map = stream(dataset).select('city').match(matcher=vocab)
    assert map['Brooklin'] == [(1.0, 'Brooklyn')]
    assert map['Quens'] == [(1.0, 'Queens')]
    # -- Error when trying to map values from more than one column ------------
    with pytest.raises(ValueError):
        stream(dataset).match(matcher=vocab)
