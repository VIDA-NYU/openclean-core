# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for phonetic string encoding algorithms."""

import pytest

from openclean.function.value.phonetic import Metaphone, NYSIIS, Soundex


@pytest.mark.parametrize(
    'func,encodings',
    [
        (Metaphone, ('BRKLN', 'JNT')),
        (NYSIIS, ('BRACLYN', 'JANAT')),
        (Soundex, ('B624', 'J530'))
    ]
)
def test_phonetic_name_encoding(func, encodings):
    """Test the encoding for names Brooklyn and Jannet using different encoding
    algorithms.
    """
    enc_brooklyn, enc_jannet = encodings
    f = func()
    assert f('Brooklyn') == enc_brooklyn
    assert f('Jannet') == enc_jannet
