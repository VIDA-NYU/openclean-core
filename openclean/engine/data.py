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
from abc import ABCMeta, abstractmethod
from histore.archive.manager.base import ArchiveManager
from typing import Dict, List, Optional, Union

import pandas as pd

from openclean.data.archive.base import Datastore
from openclean.data.types import Columns, Scalar
from openclean.engine.library.func import FunctionHandle
from openclean.engine.log import InsertOp, LogEntry, OperationLog, UpdateOp
from openclean.operator.transform.insert import inscol
from openclean.operator.transform.update import update


class DatasetHandle(metaclass=ABCMeta):
    """Handle for datasets that are managed by the openclean engine and whose
    snapshot history is maintained by an archive manager.
    """
    def __init__(self, datastore: Datastore, log: OperationLog, is_sample: bool):
        """Initialize the data store that maintains the different dataset
        snapshots.

        Parameters
        ----------
        datastore: openclean.data.archive.base.Datastore
            Datastore for managing dataset snapshots.
        log: openclean.engine.log.OperationLog
            Log for dataset operations.
        is_sample: bool
            Flag indicating if the dataset is a sample of a larger dataset.
        """
        self.datastore = datastore
        self._log = log
        self.is_sample = is_sample

    def checkout(self) -> pd.DataFrame:
        """Checkout the latest version of the dataset.

        Returns
        -------
        pd.DataFrame
        """
        return self.datastore.checkout()

    @abstractmethod
    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        raise NotImplementedError()

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
        # Checkout the current dataset snapshot.
        df = self.datastore.checkout()
        # Create an action object for the insert operation.
        action = InsertOp(
            names=names,
            pos=pos,
            values=values,
            args=args,
            sources=sources
        )
        # Run the insert operation and commit the new dataset version.
        df = inscol(df=df, names=names, pos=pos, values=action.to_eval())
        df = self.datastore.commit(df=df, action=action.to_dict())
        # Add the operator to the internal log.
        self._log.add(version=self.datastore.last_version(), action=action)
        return df

    def log(self) -> List[LogEntry]:
        """Get the list of log entries for all dataset snapshots.

        Returns
        -------
        list of openclean.engine.log.LogEntry
        """
        return list(self._log)

    def update(
        self, columns: Columns, func: FunctionHandle, args: Optional[Dict] = None,
        sources: Optional[Columns] = None
    ) -> pd.DataFrame:
        """Update a given column (or list of columns) by applying the given
        function.

        Columns defines the dataset column(s) that are being updated. If the
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
        # Checkout the current dataset snapshot.
        df = self.datastore.checkout()
        # Create an action object for the update operation.
        action = UpdateOp(
            columns=columns,
            func=func,
            args=args,
            sources=sources
        )
        # Run the update operation and commit the new dataset version.
        df = update(df=df, columns=columns, func=action.to_eval())
        df = self.datastore.commit(df=df, action=action.to_dict())
        # Add the operator to the internal log.
        self._log.add(version=self.datastore.last_version(), action=action)
        return df


class FullDataset(DatasetHandle):
    """Handle for datasets that are managed by the openclean engine and that
    have their histor being maintained by an archive manager. All operations
    are applied directly on the full dataset in the underlying archive.
    """
    def __init__(
        self, datastore: Datastore, manager: ArchiveManager, identifier: str,
        pk: Optional[Union[List[str], str]] = None
    ):
        """Initialize the reference to the datastore that maintains the history
        of the dataset that is being tranformed.

        Parameters
        ----------
        datastore: openclean.data.archive.base.Datastore
            Datastore for managing dataset snapshots.
        manager: histore.archive.manager.base.ArchiveManager
            Manager for created dataset archives.
        identifier: string, default=None
            Unique identifier of the dataset archive. The identifier is used
            to access the dataset history in the archive manager.
        pk: string or list, default=None
            Column(s) that define the primary key for the dataset. This
            information is accessed when generating a sample of the dataset
            (by the data engine).
        """
        super(FullDataset, self).__init__(
            datastore=datastore,
            log=OperationLog(snapshots=datastore.snapshots(), auto_commit=True),
            is_sample=False
        )
        self.manager = manager
        self.identifier = identifier
        self.pk = pk

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        self.manager.delete(self.identifier)


class DataSample(DatasetHandle):
    """Handle for datasets that are samples of a larger dataset. Samples datasets
    are entirely maintained in main memory.
    """
    def __init__(self, datastore: Datastore, original: DatasetHandle):
        """Initialize the reference to the datastore that maintains the history
        of the dataset that is being tranformed.

        Parameters
        ----------
        datastore: openclean.data.archive.base.Datastore
            Datastore for managing snapshots for the dataset sample.
        original: openclean.engine.data.DataEngine, default=None
            Reference to the original dataset for sampled datasets.
        """
        super(DataSample, self).__init__(
            datastore=datastore,
            log=OperationLog(snapshots=original.datastore.snapshots(), auto_commit=False),
            is_sample=True
        )
        self.original = original

    def commit(self):
        """Apply all actions in the current log to the underlying original
        dataset.
        """
        # Checkout the current snapshot for the original dataset.
        df = self.original.datastore.checkout()
        # Apply each action that is stored in the log. Removes the operation
        # from the log if it had been applied succssfully.
        for op in self.log():
            if op.is_committed:
                # Ignore log entries for committed actions.
                continue
            # Execute the operation on the latest snapshot of the original
            # dataset and commit the modified snapshot to that dataset.
            action = op.action
            if action.is_insert:
                df = inscol(df=df, names=action.names, pos=action.pos, values=action.to_eval())
            else:
                df = update(df=df, columns=action.columns, func=action.to_eval())
            df = self.original.datastore.commit(df=df, action=action.to_dict())
            op.is_committed = True
        return df

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        self.original.drop()

    def rollback(self, identifier: str) -> pd.DataFrame:
        """Rollback to the dataset version that was created by the log entry
        with the given identifier. This will make the respective snapshot the
        new current (head) snapshot for the dataset history.

        Rollback is only supported for uncommitted changes. Removes all log
        entries after the rolledback version.

        Returns the dataframe for the dataset snapshot that is at the head of
        the dataset history.

        Raises a KeyError if the given log entry identifier is unknown. Raises
        a ValueError if the log entry references a snapshot that has already
        been committed.

        Parameters
        ----------
        identifier: string
            Unique log entry identifier.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        KeyError
        ValueError
        """
        # Pointer to position of the rollback version in the full log.
        index = 0
        for op in self.log():
            if op.identifier == identifier:
                if op.is_committed:
                    raise ValueError('can only rollback uncommitted actions')
                # Make the dataset snapshot at the identified version the new
                # current snapshot.
                df = self.datastore.checkout(version=op.version)
                self.datastore.commit(df=df, action=op.action.to_dict())
                # Remove all log entries after the rollback position and set the
                # rolled back snapshot as the new head of the log.
                self._log.rollback(
                    pos=index,
                    version=self.datastore.last_version(),
                    action=op.action
                )
                # Rollback was successful. Return here to avoid a KeyError at
                # the end of the method.
                return
            index += 1
        # Raise a KeyError if no log entry with the given identifier was found.
        raise KeyError("unknown snapshot '{}'".format(identifier))
