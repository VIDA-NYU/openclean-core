# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for streaming dataset files."""

from __future__ import annotations
from typing import Callable, List

from histore.document.base import DataIterator  # noqa: F401
from histore.document.base import DataReader  # noqa: F401

from openclean.data.types import Scalar, Value


"""Type alias for stream functions. Stream functions are callables that
accept a data frame row as the only argument. They return a Value.
"""
DataRow = List[Scalar]
StreamFunction = Callable[[DataRow], Value]
