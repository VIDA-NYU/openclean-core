# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from __future__ import annotations
from histore.archive.manager.base import ArchiveManager
from histore.archive.snapshot import Snapshot
from typing import Callable, Dict, List, Optional, Union

import pandas as pd

from openclean.data.archive.base import Datastore
from openclean.data.types import Columns
from openclean.engine.library.command import CommandRegistry
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
        return self.datastore.commit(df=df, action=action)

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        if self.identifier is not None and self.manager is not None:
            self.manager.delete(self.identifier)

    def insert(self, columns, pos):
        """
        """
        pass

    def update(
        self, columns: Columns, func: Callable, args: Optional[Dict] = None,
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
        func: callable or openclean.function.eval.base.EvalFunction
            Callable that accepts a data frame row as the only argument and
            outputs a (modified) list of value(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row. Ignored if
            the function is an :class:EvalFunction.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted. Ignored if the function is an
            :class:EvalFunction.

        Returns
        -------
        pd.DataFrame
        """
        df = self.datastore.checkout()
        df = update(df=df, columns=columns, func=func)
        # df = update(df=df, columns=columns, func=func, args=args, sources=sources)
        return self.datastore.commit(df=df)

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
