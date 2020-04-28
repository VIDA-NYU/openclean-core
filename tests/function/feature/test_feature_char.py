# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string character composition features."""

from openclean.function.feature.character import (
    DigitsCount, DigitsFraction, LettersCount, LettersFraction,
    SpecCharCount, SpecCharFraction, UniqueCount, UniqueFraction,
    WhitespaceCount,  WhitespaceFraction
)


def test_digit_features():
    """Simple tests for digit character composition feature functions."""
    # -- DigitsCount ----------------------------------------------------------
    f = DigitsCount()
    assert f('123abd') == 3
    assert f(1234) == 4
    assert f('abc') == 0
    assert f('') == 0
    # -- DigitsFraction -------------------------------------------------------
    f = DigitsFraction()
    assert f('123abd') == 0.5
    assert f(1234) == 1
    assert f('abc') == 0
    assert f('') == 0


def test_letters_features():
    """Simple tests for letter character composition feature functions."""
    # -- LettersCount ---------------------------------------------------------
    f = LettersCount()
    assert f('123abd') == 3
    assert f(1234) == 0
    assert f('abc') == 3
    assert f('') == 0
    # -- LettersFraction ------------------------------------------------------
    f = LettersFraction()
    assert f('123abd') == 0.5
    assert f(1234) == 0
    assert f('abc') == 1
    assert f('') == 0


def test_specchar_features():
    """Simple tests for special character composition feature functions."""
    # -- SpecCharCount --------------------------------------------------------
    f = SpecCharCount()
    assert f('1#3$b@') == 3
    assert f(1234) == 0
    assert f('abc') == 0
    assert f('') == 0
    # -- SpecCharFraction -----------------------------------------------------
    f = SpecCharFraction()
    assert f('1#3$b@') == 0.5
    assert f(1234) == 0
    assert f('abc') == 0
    assert f('') == 0


def test_uniqueness_features():
    """Simple tests for character uniqueness feature functions."""
    # -- UniqueCount ----------------------------------------------------------
    f = UniqueCount()
    assert f('12121212') == 2
    assert f(1234) == 4
    assert f('aa') == 1
    assert f('') == 0
    # -- UniqueFraction -------------------------------------------------------
    f = UniqueFraction()
    assert f('12121212') == 0.25
    assert f(1234) == 1
    assert f('aa') == 0.5
    assert f('') == 0


def test_whitespace_features():
    """Simple tests for whitespace character composition feature functions."""
    # -- WhitespaceCount ------------------------------------------------------
    f = WhitespaceCount()
    assert f('1 2 3 ') == 3
    assert f(1234) == 0
    assert f('   ') == 3
    assert f('') == 0
    # -- WhitespaceFraction ---------------------------------------------------
    f = WhitespaceFraction()
    assert f('1 2 3 ') == 0.5
    assert f(1234) == 0
    assert f('   ') == 1
    assert f('') == 0
