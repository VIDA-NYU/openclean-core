# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for regular expression discovery operators."""

from abc import ABCMeta, abstractmethod


class RegularExpressionFinder(metaclass=ABCMeta):
    """Interface for generic regular expression discovery. Each implementation
    should take an interable of (distinct) values (e.g., from a column in a
    data frame) as their input. The result is a (list of) string(s) that each
    represent a regular expression.
    """
    @abstractmethod
    def find(self, values):
        """Discover regular expressions in a given sequence of (distinct)
        values. Returns a list of strings representing regular expressions in
        the Python Regular Expression Syntax

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        raise NotImplementedError()
