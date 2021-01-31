# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that compute feature valuezs for scalar cell values
base on the character composition of the string representation a value.
"""


def fraction(count, value):
    """Divides the given character count by the length of the string
    representation for the given value.

    Parameters
    ----------
    count: int
        Character count returned by one of the count functions.
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    strlen = len(str(value))
    if strlen > 0:
        return count / strlen
    else:
        return 0


def digits_count(value):
    """Count the number of digits in the string representation of a scalar
    value.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    int
    """
    return sum(c.isdigit() for c in str(value))


def digits_fraction(value):
    """Compute the fraction of characters in a string value that are digits.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    return fraction(digits_count(value), value)


def letters_count(value):
    """Count the number of letters in the string representation of a scalar
    value.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    int
    """
    return sum(c.isalpha() for c in str(value))


def letters_fraction(value):
    """Compute the fraction of characters in a string value that are letters.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    return fraction(letters_count(value), value)


def spec_char_count(value):
    """Count the number of characters in the string representation of a scalar
    value that are not digit, letter, or white space.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    int
    """
    count = 0
    for c in str(value):
        if not (c.isdigit() or c.isalpha() or c.isspace()):
            count += 1
    return count


def spec_char_fraction(value):
    """Compute the fraction of characters in a string value that are not
    digits, letters, or white space characters.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    return fraction(spec_char_count(value), value)


def unique_count(value):
    """Count the number of unique characters in the string representation of a
    scalar value.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    int
    """
    unique = set()
    for c in str(value):
        unique.add(c)
    return len(unique)


def unique_fraction(value):
    """Compute the uniqueness of characters for a string value.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    return fraction(unique_count(value), value)


def whitespace_count(value):
    """Count the number of white space characters in the string representation
    for a scalar value.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    int
    """
    return sum(c.isspace() for c in str(value))


def whitespace_fraction(value):
    """Compute the fraction of characters in a string value that are white
    space characters.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    float
    """
    return fraction(whitespace_count(value), value)
