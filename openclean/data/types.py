# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type alias for basic data types."""

from datetime import datetime
from typing import Union


# Scalar values.
Scalar = Union[int, float, str, datetime]
