# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Repair function for groups of rows that represent constraint violations."""

from typing import Dict

import pandas as pd

from openclean.data.groupby import DataFrameViolation
from openclean.data.schema import column_ref
from openclean.data.types import Value
from openclean.function.value.base import ValueFunction


"""Import default conflict resolution functions."""
from openclean.function.value.aggregate import Longest, Max, Min, Shortest  # noqa: F401
from openclean.function.value.random import RandomSelect as Random  # noqa: F401
from openclean.function.value.vote import MajorityVote as Vote  # noqa: F401


def conflict_repair(
    conflicts: DataFrameViolation, strategy: Dict[Value, ValueFunction]
) -> pd.DataFrame:
    """The conflict repair function resolves conflicts in data frames (groups)
    that contain sets of rows that together represent a single violation of a
    functional dependency constraint. The function resolves conflicts by
    consolidating values in the (conflicting) data frame columns using a given
    set of conflict resolution functions (strategy).

    The idea is that the user specifies a conflict resolution function for each
    attribute that has multiple values which form a violation of a checked
    constraint (FD). The conflict resolution strategy is a mapping of column
    names (or index positions) to value functions for conflict resolution. It is
    up to the user for which columns they want to provide conflict resolutions
    functions.

    The conflict resolution functions are applied on the respective attribute
    for each data frame (group) that represents a constraint violation. The
    modified rows are merged with the remainin (non-conflicting) rows in the
    data frame that was used for voliation detection. The resuling data frame is
    returned as the result of the the repair function.

    Parameters
    ----------
    conflicts: openclean.data.groupby.DataFrameViolation
        Grouping of rows from a data frame. Each group represents a set of rows
        that form a violation of a checked integrity constraint.
    strategy: dict
        Mapping of column names or index positions to conflict resolution
        functions.

    Returns
    -------
    pd.DataFrame
    """
    # Get the data frame that was used as input for violoation detection.
    df = conflicts.df
    # The strategy may reference columns in the original data frame by name or
    # by index position. Replace all mappings from keys that are names (str) to
    # the respective index position.
    schema = list(df.columns)
    resolution_functions = dict()
    for key, func in strategy.items():
        if isinstance(key, str):
            _, key = column_ref(schema=schema, column=key)
        resolution_functions[key] = func
    # Instanciate a different set of resolution functions for each group of
    # conflicting rows. That is, with the key for each group of violations we
    # associate a separate list of instances of the resoultion functions that
    # are prepared individually for the respective group.
    conflict_resolver = dict()
    # We also maintain an inverted index of row index positions to the conflict
    # group they belong to.
    row_index = dict()
    for key in conflicts.keys():
        group = conflicts.get(key)
        # Create set of prepared resolution functions for the group.
        prep_functions = dict()
        for colidx, func in resolution_functions.items():
            if not func.is_prepared():
                prep_functions[colidx] = func.prepare(list(group.iloc[:, colidx]))
            else:
                prep_functions[colidx] = func
        conflict_resolver[key] = prep_functions
        # Add group rows to inverted row index.
        for rowidx in conflicts.rows(key):
            row_index[rowidx] = key
    # Crete a new data frame where values in rows that belong to conflict groups
    # are updated by the respective set on prepared conflict resolution functions.
    rowidx = 0
    data = list()
    index = list()
    for rowid, values in df.iterrows():
        if rowidx in row_index:
            # Update the values in the data row.
            for colidx, f in conflict_resolver[row_index[rowidx]].items():
                values[colidx] = f(values[colidx])
        data.append(values)
        index.append(rowid)
        rowidx += 1
    return pd.DataFrame(data=data, index=index, columns=schema)
