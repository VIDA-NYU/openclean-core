# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Interfaces for string tokenizer and token set transformers."""

from abc import ABCMeta, abstractmethod
from typing import List

from openclean.data.types import Scalar


class StringTokenizer(metaclass=ABCMeta):
    """Interface for string tokenizer. A string tokenizer should be able to
    handle any scalar value (e.g., by first transforming numeric values into
    a string representation). The tokenizer returns a list of string values.
    """
    @abstractmethod
    def tokens(self, value: Scalar) -> List[str]:
        """Convert a given scalar values into a list of string tokens. If a
        given value cannot be converted into tokens None should be returned.

        The order of tokens in the returned list not necissarily corresponds to
        their order in the original value. This is implementation dependent.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of tokens.

        Returns
        -------
        list of string
        """
        raise NotImplementedError()  # pragma: no cover
