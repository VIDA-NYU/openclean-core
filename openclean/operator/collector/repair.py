# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Repair function for groups of rows that represent constraint violations."""

from typing import Dict, Optional

import pandas as pd

from openclean.data.groupby import DataFrameGrouping
from openclean.data.schema import column_ref
from openclean.data.stream.base import StreamFunction
from openclean.data.types import DatasetSchema, Value
from openclean.function.eval.base import EvalFunction, EvalResult
from openclean.function.value.base import ValueFunction
from openclean.operator.base import DataGroupReducer
from openclean.operator.transform.update import update


# Import default conflict resolution functions.
from openclean.function.value.aggregate import Longest, Max, Min, Shortest  # noqa: F401
from openclean.function.value.random import RandomSelect as Random  # noqa: F401
from openclean.function.value.vote import MajorityVote as Vote  # noqa: F401


def conflict_repair(
    conflicts: DataFrameGrouping, strategy: Dict[Value, ValueFunction],
    in_order: Optional[bool] = True
) -> pd.DataFrame:
    """The conflict repair function resolves conflicts in data frames (groups)
    that contain sets of rows that together represent a single violation of a
    functional dependency constraint. The function resolves conflicts by
    consolidating values in the (conflicting) data frame columns using a given
    set of conflict resolution functions (strategy).

    The idea is that the user specifies a conflict resolution function for each
    attribute that has multiple values which form a violation of a constraint
    (e.g., a functional dependency). The conflict resolution strategy is defined
    as a mapping of column names (or index positions) to value functions for
    conflict resolution. It is up to the user for which columns they want to
    provide conflict resolutions functions.

    The conflict resolution functions are applied on the respective attribute
    for each data frame (group) that represents a constraint violation. The
    modified rows are merged with the remainin (non-conflicting) rows in the
    data frame that was used for voliation detection. The resuling data frame is
    returned as the result of the the repair function.

    The in_order flag determines the algorithm variant that is used to modify the
    given data frame. If in_order is True the rows in the resulting data frame
    are in the same order as in the input data frame. This is achieved by creating
    a copy of the data frame and updating rows in place. If in_order is False
    the rows for the updated groups are appened to a data frame that initially
    contains only the non-conflicting rows. Therefore it is likely that the
    rows in the resulting data frame are in different order than in the input
    data frame.

    Parameters
    ----------
    conflicts: openclean.data.groupby.DataFrameGrouping
        Grouping of rows from a data frame. Each group represents a set of rows
        that form a violation of a checked integrity constraint.
    strategy: dict
        Mapping of column names or index positions to conflict resolution
        functions.
    in_order: bool, default=True
        Only if the in_order flag is True the resulting data frame is guaranteed
        to have the rows in the same order as the input data frame.

    Returns
    -------
    pd.DataFrame
    """
    return ConflictRepair(strategy=strategy, in_order=in_order).reduce(conflicts)


class ConflictRepair(DataGroupReducer):
    """The conflict repair function resolves conflicts in data frames (groups)
    that contain sets of rows that together represent a single violation of a
    functional dependency constraint. The function resolves conflicts by
    consolidating values in the (conflicting) data frame columns using a given
    set of conflict resolution functions (strategy).
    """
    def __init__(
        self, strategy: Dict[Value, ValueFunction], in_order: Optional[bool] = True
    ):
        """Initialize the conflict resolution strategy and the flag that determines
        the algorithm variant.

        Parameters
        ----------
        strategy: dict
            Mapping of column names or index positions to conflict resolution
            functions.
        in_order: bool, default=True
            Only if the in_order flag is True the resulting data frame is guaranteed
            to have the rows in the same order as the input data frame.
        """
        self.strategy = strategy
        self.in_order = in_order

    def reduce(self, groups: DataFrameGrouping) -> pd.DataFrame:
        """The conflict resolution functions are applied on the respective attribute
        for each data frame (group) that represents a constraint violation. The
        modified rows are merged with the remainin (non-conflicting) rows in the
        data frame that was used for voliation detection. The resuling data frame
        is returned as the result of the the repair function.

        The in_order flag determines the algorithm variant that is used to modify
        the given data frame. If in_order is True the rows in the resulting data
        frame are in the same order as in the input data frame. This is achieved
        by creating a copy of the data frame and updating rows in place. If in_order
        is False, the rows for the updated groups are appened to a data frame that
        initially contains only the non-conflicting rows.

        Parameters
        ----------
        groups: openclean.data.groupby.DataFrameGrouping
            Grouping of pandas data frames.

        Returns
        -------
        pd.DataFrame
        """
        # Get the data frame that was used as input for violoation detection.
        df = groups.df
        # The strategy may reference columns in the original data frame by name
        # or by index position. Replace all mappings from keys that are names
        # (str) to the respective index position.
        schema = list(df.columns)
        resolution_functions = dict()
        for key, func in self.strategy.items():
            if isinstance(key, str):
                _, key = column_ref(schema=schema, column=key)
            resolution_functions[key] = func
        # Update the rows in conflict groups. The update algorithm  that is used
        # depends on the value of the in_order flag.
        if self.in_order:
            return _repair_conflicts_in_place(
                df=df.copy(deep=True),
                conflicts=groups,
                strategy=resolution_functions
            )
        else:
            return _repair_conflicts_with_concat(
                df=df,
                conflicts=groups,
                strategy=resolution_functions
            )


# -- Helper Functions and Classes ---------------------------------------------

class ValueExtractor(EvalFunction):
    """Helper class that extracts and manipulates column values using a list of
    prepared value functions.
    """
    def __init__(self, strategy: Dict[int, ValueFunction]):
        """Initialize the set of functions. The given dictionary maps the column
        index to the function that is used to modify the column values. All
        functions are expected to have been prepared.

        Parameters
        ----------
        strategy: dict
            Mapping of column index positions to prepared conflict resolution
            functions.
        """
        self.functions = strategy

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Evaluate the value functions on the values of their respective
        columns. The column values are extracted from the given data frame.
        The respective value function is then prepared using the column values
        (if necessary). At last, the column values are modified using the prepared
        value function.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        pd.Series or list
        """
        # Maintain all updated columns in a list of lists.
        data = list()
        for colidx, func in self.functions.items():
            values = list(df.iloc[:, colidx])
            if not func.is_prepared():
                func = func.prepare(values)
            data.append(func.apply(values))
        # Return updated values. Need to zip result if more than one column was
        # modified.
        return list(zip(*data)) if len(data) > 1 else data[0]

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
        """The function mapping already contains references to columns by their
        index position. There is nothing to prepare. We raise an error because
        the ValueExtractor is not intended to be used as a stream function at
        this point.

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """
        raise NotImplementedError()


def _prepare_strategy(
    df: pd.DataFrame, strategy: Dict[int, ValueFunction], prepare: Optional[bool] = True
) -> Dict[int, ValueFunction]:
    """Convert references to columns by their name to their index position.
    Prepares the value functions if the prepare flag is True. Returns a mapping
    of column indexes to (prepared) value functions.
    """
    result = dict()
    for colidx, func in strategy.items():
        if not func.is_prepared() and prepare:
            result[colidx] = func.prepare(list(df.iloc[:, colidx]))
        else:
            result[colidx] = func
    return result


def _repair_conflicts_in_place(
    df: pd.DataFrame, conflicts: DataFrameGrouping, strategy: Dict[int, ValueFunction]
) -> pd.DataFrame:
    """Repairs conflicts by updating rows that belong to conflict groups in place.
    This function modifies the given data frame and returns it. It thereby
    ensures that the order of rows does not change during conflict resolution.
    """
    # For each group of conflicts, instantiate the conflict resolution functions
    # and then update all rows in the group in place.
    for key in conflicts.keys():
        group = conflicts.get(key)
        # Create set of prepared resolution functions for the group.
        prep_functions = _prepare_strategy(df=group, strategy=strategy)
        # Update rows in place.
        for rowidx in conflicts.rows(key):
            values = list(df.iloc[rowidx])
            for colidx, f in prep_functions.items():
                values[colidx] = f(values[colidx])
            df.iloc[rowidx] = values
    # Return the modified input data frame.
    return df


def _repair_conflicts_with_concat(
    df: pd.DataFrame, conflicts: DataFrameGrouping, strategy: Dict[int, ValueFunction]
) -> pd.DataFrame:
    # For each group of conflicts, instantiate the conflict resolution functions
    # and then create an updated data frame for the group. The updated groups
    # are appended to the groups list. We also keep a list of rows that are
    # unchanged to be included later in the returned data frame.
    unchanged_rows = [True] * df.shape[0]
    groups = list()
    for key in conflicts.keys():
        group = conflicts.get(key)
        # Create mapping of column index positions to unprepared resolution
        # functions for the group. The functions will be prepared by the
        # value extractor to avoid having to extract values for each column
        # more than once.
        prep_functions = _prepare_strategy(df=group, strategy=strategy, prepare=False)
        columns = [colidx for colidx, _ in prep_functions.items()]
        func = ValueExtractor(strategy=prep_functions)
        groups.append(update(df=group, columns=columns, func=func))
        # Create an updated data frame.
        for rowidx in conflicts.rows(key):
            unchanged_rows[rowidx] = False
    # Return the modified input data frame.
    return pd.concat([df[unchanged_rows]] + groups)
