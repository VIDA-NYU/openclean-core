# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Columns in openclean data frames have a unique identifier and a column name.
The column class extends the Python String class to be able to be used as a
column value in a Pandas data frame.
"""

from typing import List, Union

# Openclean makes use of the identifiable column name that is also defined in
# HISTORE.
from histore.document.schema import Column as Column  # noqa: F401


class ColumnSet(list):
    """List of columns (objects or strings) from a data frame schema. This is a
    simple wrapper around a Python list that allows to ensure that the names of
    all columns are unique. The set is also aware of columns that are objects
    of the identifiable column type.
    """
    def __init__(
        self, columns: List[Union[str, Column]] = None,
        duplicate_ok: bool = True
    ):
        """Initialize the set of columns. Raises a ValueError if the elements
        in the list are not unique and the duplicate_ok flag is False.

        Parameters
        ----------
        columns: list, default=None
            List of column names or identifiable columns.
        duplicate_ok: bool, default=True
            Raise a ValueError if the flag is True and the given column list
            contains duplicate entries.
        """
        if columns is not None:
            for col in columns:
                if col in self and not duplicate_ok:
                    raise ValueError('duplicate column {}'.format(col))
                self.append(col)
