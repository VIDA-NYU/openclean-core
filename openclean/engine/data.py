# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The data engine is used to manipulate a dataset with insert and update
operations that use functions from the command registry.
"""


from __future__ import annotations
from histore.archive.manager.base import ArchiveManager
from histore.archive.snapshot import Snapshot
from typing import Dict, List, Optional, Union

import pandas as pd

from openclean.data.archive.base import Datastore
from openclean.data.types import Columns, Scalar
from openclean.engine.library.command import CommandRegistry
from openclean.engine.library.func import FunctionHandle
from openclean.engine.log import InsertOp, OperationLog, UpdateOp
from openclean.operator.transform.insert import inscol
from openclean.operator.transform.update import update


class DataEngine(object):
    """Handle for datasets that are maintained by the openclean engine.

    The archive identifier and manager are only set for persisted datasets.
    This informaiton is required to delete all resources that are associated
    with a dataset history.
    """
    def __init__(
        self, datastore: Datastore, commands: CommandRegistry,
        identifier: Optional[str] = None, manager: Optional[ArchiveManager] = None,
        pk: Optional[Union[List[str], str]] = None, original: Optional[DataEngine] = None
    ):
        """Initialize the reference to the datastore that maintains the history
        of the dataset that is being tranformed.

        Parameters
        ----------
        datastore: openclean.data.archive.base.Datastore
            Datastore for managing dataset snapshots.
        """
        self.datastore = datastore
        self.commands = commands
        self.identifier = identifier
        self.manager = manager
        self.pk = pk
        self.original = original
        # Log for operations that have been applied to the dataset.
        self.log = OperationLog()

    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Get a specific version of a dataset. Raises a ValueError if the
        given version number is unknown.

        Parameters
        ----------
        version: int
            Unique dataset version identifier.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
        """
        return self.datastore.checkout(version=version)

    def commit(self, df: pd.DataFrame, action: Optional[Dict] = None) -> pd.DataFrame:
        """Insert a new version for a dataset.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame containing the new dataset version that is being stored.
        action: dict, default=None
            Optional description of the action that created the new dataset
            version.

        Returns
        -------
        pd.DataFrame
        """
        # Cannot commit new data frame version to sampled data set. A dataset
        # represents a sample if it has an original dataset.
        if self.original is not None:
            raise RuntimeError('cannot commit to sampled dataset')
        return self.datastore.commit(df=df, action=action)

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        if self.identifier is not None and self.manager is not None:
            self.manager.delete(self.identifier)

    def insert(
        self, names: Union[str, List[str]], pos: Optional[int] = None,
        values: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ) -> pd.DataFrame:
        """Insert one or more columns at a given position into the dataset. One
        column is inserted for each given column name. If the insert position is
        undefined, columns are appended. If the position does not reference
        a valid position (i.e., not between 0 and len(df.columns)) a ValueError
        is raised.

        Values for the inserted columns are generated using a given constant
        value or function. If a function is given, it is expected to return
        exactly one value (e.g., a tuple of len(names)) for each of the inserted
        columns.

        Parameters
        ----------
        names: string, or list(string)
            Names of the inserted columns.
        pos: int, default=None
            Insert position for the new columns. If None, the columns will be
            appended.
        values: scalar or openclean.engine.library.func.FunctionHandle, default=None
            Single value, tuple of values, or library function that is used to
            generate the values for the inserted column(s). If no default is
            specified all columns will contain None.
        func: callable or openclean.function.eval.base.EvalFunction
            Callable that accepts a data frame row as the only argument and
            outputs a (modified) list of value(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.

        Returns
        -------
        pd.DataFrame
        """
        action = InsertOp(names=names, pos=pos, values=values, args=args, sources=sources)
        df = self.datastore.checkout()
        df = inscol(df=df, names=names, pos=pos, values=action.to_eval())
        df = self.datastore.commit(df=df, action=action.to_dict())
        self.log.add(version=self.datastore.last_version(), action=action)
        return df

    def update(
        self, columns: Columns, func: FunctionHandle, args: Optional[Dict] = None,
        sources: Optional[Columns] = None
    ) -> pd.DataFrame:
        """Update a given column (or list of columns) by applying the given
        function.

        Columns defines the dataste column(s) that are being updated. If the
        given function is an evaluation function, that function will define the
        columns from which the input values are being retrieved. If the function
        is not an evaluation function, the input values for the update function
        will come from the same column(s) that are being modified. This behavior
        can be changed by specifying a list of source columns. If function is
        a callable (not an evaluation function) and sources is given, row values
        from the column(s) that are specified by `sounrces` are used as the input
        to the update function.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        func: openclean.engine.library.func.FunctionHandle
            Library function that is used to generate modified values for the
            updated column(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.

        -------
        pd.DataFrame
        """
        action = UpdateOp(columns=columns, func=func, args=args, sources=sources)
        df = self.datastore.checkout()
        df = update(df=df, columns=columns, func=action.to_eval())
        df = self.datastore.commit(df=df, action=action.to_dict())
        self.log.add(version=self.datastore.last_version(), action=action)
        return df

    def snapshots(self) -> List[Snapshot]:
        """Get list of snapshot handles for all versions of the dataset.

        Returns
        -------
        list of histore.archive.snapshot.Snapshot

        Raises
        ------
        ValueError
        """
        return self.datastore.snapshots()
