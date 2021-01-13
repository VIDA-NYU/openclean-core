# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for text normalization functions."""

import pytest

from openclean.function.token.key import Fingerprint


@pytest.mark.parametrize(
    'text,result',
    [
        (u'M\u00FCll\u00E9r, Heiko', 'heiko muller'),
        ('New \t York City.', 'city new york'),
        ('5TH AVE, NY.', '5th ave ny')
    ]
)
def test_text_normalizer(text, result):
    """Test the text normalization function."""
    assert Fingerprint().eval(text) == result
