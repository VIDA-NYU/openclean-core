# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type alias for basic data types.

Columns in openclean data frames have a unique identifier and a column name.
The column class extends the Python String class to be able to be used as a
column value in a Pandas data frame.
"""

from datetime import datetime
from typing import Tuple, Union


# Openclean makes use of the identifiable column name that is also defined in
# HISTORE.
from histore.document.schema import Column, Columns, ColumnRef  # noqa: F401
from histore.document.schema import DocumentSchema as DatasetSchema  # noqa: F401

# Scalar values.
Scalar = Union[int, float, str, datetime]
# Elements in an input stream generated from one or morce columns in a dataset.
Value = Union[Scalar, Tuple[Scalar]]
