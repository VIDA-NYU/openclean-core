# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for key generator functions."""

import pytest

from openclean.function.value.key.fingerprint import Fingerprint, NGramFingerprint


@pytest.mark.parametrize(
    'text,result',
    [
        (u'M\u00FCll\u00E9r, Heiko', 'heiko muller'),
        ('New \t York City.', 'city new york'),
        ('5TH AVE, NY.', '5th ave ny')
    ]
)
def test_default_fingerprint_key(text, result):
    """Test default fingerprint key generator."""
    assert Fingerprint().eval(text) == result


@pytest.mark.parametrize(
    'text,pleft,pright,result',
    [
        ('ABC', None, None, 'abc'),
        ('ABC', '$', '#', '$$a $ab abc bc# c##'),
        ('5TH AVE,', '$', '#', '$$5 $5t 5th th  h a  av ave ve# e##')
    ]
)
def test_ngram_fingerprint_key(text, pleft, pright, result):
    """Test 3-gram key generator."""
    assert NGramFingerprint(n=3, pleft=pleft, pright=pright).eval(text) == result
