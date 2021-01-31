# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for text normalization functions."""

import pytest

from openclean.function.value.normalize.text import TextNormalizer


@pytest.mark.parametrize(
    'text,result',
    [
        (u'M\u00FCll\u00E9r', 'muller'),
        ('New York', 'new york'),
        ('5TH AVE,  \t NY.', '5th ave ny'),
        ('Da\u00DF', 'dass')
    ]
)
def test_text_normalizer(text, result):
    """Test the text normalization function."""
    assert TextNormalizer().eval(text) == result
