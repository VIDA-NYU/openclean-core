# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for default tokenizer."""

import pytest

from openclean.function.token.base import Tokens, TokenPrefix
from openclean.function.token.split import Split


@pytest.mark.parametrize(
    'tokens,result',
    [
        (Tokens(Split(' ')), 'ADECFBA'),
        (Tokens(Split(' '), sort=True), 'AABCDEF'),
        (Tokens(Split(' '), sort=True, reverse=True), 'FEDCBAA'),
        (Tokens(Split(' '), unique=True, sort=True), 'ABCDEF'),
        (Tokens(Split(' '), transformer=TokenPrefix(2)), 'AD'),
        (Tokens(Split(' '), transformer=TokenPrefix(2), reverse=True), 'DA')
    ]
)
def test_default_tokenizer(tokens, result):
    """Test the default tokenenizer without providing ."""
    assert tokens('A D E C F B A') == result


def test_delim_parameter():
    """Test to ensure thate the delimiter parameter is used correctly."""
    value = 'A B C'
    # Default delimiter is ''
    tokens = Tokens(Split(' '))
    assert tokens(value) == 'ABC'
    # Use alternative delimiter '|'
    tokens = Tokens(Split(' '), delim='|')
    assert tokens(value) == 'A|B|C'
