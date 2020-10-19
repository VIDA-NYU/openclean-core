# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type alias for basic data types.

Columns in openclean data frames have a unique identifier and a column name.
The column class extends the Python String class to be able to be used as a
column value in a Pandas data frame.
"""

from datetime import datetime
from typing import List, Tuple, Union


# Openclean makes use of the identifiable column name that is also defined in
# HISTORE.
from histore.document.schema import Column as Column  # noqa: F401


# Column names are strings or column objects.
ColumnName = Union[str, Column]
# Reference to a column in a data frame schema. Columns can be referenced by
# their name or index position in the dataset schema.
ColumnRef = Union[int, str, Column]
# List of columns that are identified either by their name or index position,
# or represented as a Column object. A single index (int) or column name (str)
# are also accepted as 'a list with a single element'.
Columns = Union[ColumnRef, List[ColumnRef]]


# Scalar values.
Scalar = Union[int, float, str, datetime]
# Elements in an input stream generated from one or morce columns in a dataset.
Value = Union[Scalar, Tuple[Scalar]]
