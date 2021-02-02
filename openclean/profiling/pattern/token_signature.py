# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Token signatures for column type classification and anomaly detection.

Token signatures are sets of tokens that are representative for values in a
semantic type. A common example are street addresses. In the U.S., for example,
the values in dataset columns that contain street addresses are likely to
contain tokens like 'AVENUE', 'ROAD', 'STREET', etc. In many cases alternative
abbreviations are possible for these tokens, e.g., 'STREET', 'STRT', 'STR', etc.

A token signature is a list of sets, where each set contains the different
possible abbreviations for a token that is part of a representative signature
for a semantic type.

When used for column type classification, for example, one would expect that
values in a column of the classified type are likely to contain exactly one
representation for one of the tokens in the type signature.
"""

from typing import List, Optional, Set

from openclean.data.groupby import DataFrameGrouping
from openclean.data.schema import select_clause
from openclean.data.types import Columns


# -- Type alias for token signatures ------------------------------------------

TokenSignature = List[Set[str]]


def token_signature(
    grouping: DataFrameGrouping, columns: Columns,
    include_key: Optional[bool] = True
) -> TokenSignature:
    """Create a token signature from the specified columns in a data frame
    grouping.

    Each group represents an entry in the returned signature. The set of
    distinct values from all columns over the rows in the group represent the
    signature entry with the different token representations.

    Parameters
    ----------
    grouping: openclean.data.groupby.DataFrameGrouping
        Grouping of data frame rows.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    include_key: bool, default=True
        Include the key value for each group in the signature entry that is
        being created for the group.

    Returns
    -------
    openclean.profiling.pattern.token_signature.TokenSignature
    """
    result = list()
    # Get the index position for the columns that contain the values that will
    # be included in the token signature.
    _, colidx = select_clause(schema=grouping.columns, columns=columns)
    for key in grouping.keys():
        df = grouping.get(key)
        # Make sure to include the group key if the respective flag is True.
        values = set([key]) if include_key else set()
        # Iterate over group values for all specified columns and add the
        # values to the token set.
        for idx in colidx:
            for value in df.iloc[:, idx]:
                values.add(value)
        # Add set of distinct values to the result.
        result.append(values)
    return result
