# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string character composition features."""

from openclean.embedding.feature.character import (
    digits_count, digits_fraction, letters_count, letters_fraction,
    spec_char_count, spec_char_fraction, unique_count, unique_fraction,
    whitespace_count,  whitespace_fraction
)


def test_digit_features():
    """Simple tests for digit character composition feature functions."""
    # -- DigitsCount ----------------------------------------------------------
    assert digits_count('123abd') == 3
    assert digits_count(1234) == 4
    assert digits_count('abc') == 0
    assert digits_count('') == 0
    # -- DigitsFraction -------------------------------------------------------
    assert digits_fraction('123abd') == 0.5
    assert digits_fraction(1234) == 1
    assert digits_fraction('abc') == 0
    assert digits_fraction('') == 0


def test_letters_features():
    """Simple tests for letter character composition feature functions."""
    # -- LettersCount ---------------------------------------------------------
    assert letters_count('123abd') == 3
    assert letters_count(1234) == 0
    assert letters_count('abc') == 3
    assert letters_count('') == 0
    # -- LettersFraction ------------------------------------------------------
    assert letters_fraction('123abd') == 0.5
    assert letters_fraction(1234) == 0
    assert letters_fraction('abc') == 1
    assert letters_fraction('') == 0


def test_specchar_features():
    """Simple tests for special character composition feature functions."""
    # -- SpecCharCount --------------------------------------------------------
    assert spec_char_count('1#3$b@') == 3
    assert spec_char_count(1234) == 0
    assert spec_char_count('abc') == 0
    assert spec_char_count('') == 0
    # -- SpecCharFraction -----------------------------------------------------
    assert spec_char_fraction('1#3$b@') == 0.5
    assert spec_char_fraction(1234) == 0
    assert spec_char_fraction('abc') == 0
    assert spec_char_fraction('') == 0


def test_uniqueness_features():
    """Simple tests for character uniqueness feature functions."""
    # -- UniqueCount ----------------------------------------------------------
    assert unique_count('12121212') == 2
    assert unique_count(1234) == 4
    assert unique_count('aa') == 1
    assert unique_count('') == 0
    # -- UniqueFraction -------------------------------------------------------
    assert unique_fraction('12121212') == 0.25
    assert unique_fraction(1234) == 1
    assert unique_fraction('aa') == 0.5
    assert unique_fraction('') == 0


def test_whitespace_features():
    """Simple tests for whitespace character composition feature functions."""
    # -- WhitespaceCount ------------------------------------------------------
    assert whitespace_count('1 2 3 ') == 3
    assert whitespace_count(1234) == 0
    assert whitespace_count('   ') == 3
    assert whitespace_count('') == 0
    # -- WhitespaceFraction ---------------------------------------------------
    assert whitespace_fraction('1 2 3 ') == 0.5
    assert whitespace_fraction(1234) == 0
    assert whitespace_fraction('   ') == 1
    assert whitespace_fraction('') == 0
