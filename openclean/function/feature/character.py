# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that compute feature valuezs for scalar cell values
base on the character composition of the string representation a value.
"""

from abc import ABCMeta, abstractmethod


# -- Character counter interface ----------------------------------------------

class CharCounter(metaclass=ABCMeta):
    @abstractmethod
    def count(self, value):
        """Returns the number of characters in the string representation of the
        given value that satisfy an implementation-specific selection criteria.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        int
        """
        raise NotImplementedError()

    def fraction(self, value):
        """Returns the fraction of characters in the string representation of
        the given value that satisfy an implementation-specific criteria.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        strlen = len(str(value))
        if strlen > 0:
            return self.count(value) / strlen
        else:
            return 0


class DigitsCount(CharCounter):
    """Count the number of digits in the string representation of a scalar
    value.
    """
    def count(self, value):
        """Returns the number of digits in the string representation of the
        given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        int
        """
        return sum(c.isdigit() for c in str(value))

    __call__ = count


class DigitsFraction(DigitsCount):
    """Compute the fraction of characters in a string value that are digits."""
    def __call__(self, value):
        """Returns the fraction of digits in the string representation of the
        given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        return self.fraction(value)


class LettersCount(CharCounter):
    """Count the number of letters in the string representation of a scalar
    value.
    """
    def count(self, value):
        """Returns the number of letters in the string representation of the
        given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        int
        """
        return sum(c.isalpha() for c in str(value))

    __call__ = count


class LettersFraction(LettersCount):
    """Compute the fraction of characters in a string value that are letters.
    """
    def __call__(self, value):
        """Returns the fraction of letters in the string representation of the
        given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        return self.fraction(value)


class SpecCharCount(CharCounter):
    """Count the number of characters in the string representation of a scalar
    value that are not digit, letter, or white space.
    """
    def count(self, value):
        """Returns the number of characters in the string representation of the
        given value that are not digit, letter, or white space.

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

    __call__ = count


class SpecCharFraction(SpecCharCount):
    """Compute the fraction of characters in a string value that are not
    digits, letters, or white space characters.
    """
    def __call__(self, value):
        """Returns the fraction of characters in the string representation of
        the given value that are not digits, letters, or white space.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        return self.fraction(value)


class UniqueCount(CharCounter):
    """Count the number of unique characters in the string representation of a
    scalar value.
    """
    def count(self, value):
        """Returns the number of unique characters in the string representation
        of the given value.

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

    __call__ = count


class UniqueFraction(UniqueCount):
    """Compute the uniqueness of characters for a string value."""
    def __call__(self, value):
        """Returns the uniqueness of characters in the string representation of
        the given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        return self.fraction(value)


class WhitespaceCount(CharCounter):
    """Count the number of white space characters in the string representation
    for a scalar value.
    """
    def count(self, value):
        """Returns the number of white space characters in the string
        representation of the given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        int
        """
        return sum(c.isspace() for c in str(value))

    __call__ = count


class WhitespaceFraction(WhitespaceCount):
    """Compute the fraction of characters in a string value that are white
    space characters.
    """
    def __call__(self, value):
        """Returns the fraction of white space characters in the string
        representation of the given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        float
        """
        return self.fraction(value)
