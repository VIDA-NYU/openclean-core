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

from openclean.data.archive.base import ArchiveStore
from openclean.data.metadata.base import MetadataStore
from openclean.data.metadata.mem import VolatileMetadataStore
from openclean.data.types import Columns, Scalar
from openclean.engine.action import OpHandle, InsertOp, UpdateOp
from openclean.engine.object.function import FunctionHandle
from openclean.engine.log import LogEntry, OperationLog
from openclean.operator.transform.insert import inscol
from openclean.operator.transform.update import update


class DatasetHandle(metaclass=ABCMeta):
    """Handle for datasets that are managed by the openclean engine and whose
    snapshot history is maintained by an archive manager.
    """
    def __init__(self, log: OperationLog, is_sample: bool):
        """Initialize the operation log and the flag that distinguishes dataset
        samples from full datasets.

        Parameters
        ----------
        log: openclean.engine.log.OperationLog
            Log for dataset operations.
        is_sample: bool
            Flag indicating if the dataset is a sample of a larger dataset.
        """
        self._log = log
        self.is_sample = is_sample

    @abstractmethod
    def add_snapshot(self, df: pd.DataFrame, action: OpHandle) -> pd.DataFrame:  # pragma: no cover
        """Add a new snapshot to the history of the dataset. Returns the data
        frame for the snapshot.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame fr the new dataset snapshot.
        action: openclean.engine.action.OpHandle
            Operator that created the dataset snapshot.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()

    @abstractmethod
    def checkout(self) -> pd.DataFrame:  # pragma: no cover
        """Checkout the latest version of the dataset.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()

    @abstractmethod
    def drop(self):  # pragma: no cover
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
        values: scalar or openclean.engine.object.func.FunctionHandle, default=None
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
        df = self.checkout()
        # Create an action object for the insert operation.
        action = InsertOp(
            schema=list(df.columns),
            names=names,
            pos=pos,
            values=values,
            args=args,
            sources=sources
        )
        # Run the insert operation and commit the new dataset version.
        df = inscol(df=df, names=names, pos=pos, values=action.to_eval())
        return self.add_snapshot(df=df, action=action)

    def log(self) -> List[LogEntry]:
        """Get the list of log entries for all dataset snapshots.

        Returns
        -------
        list of openclean.engine.log.LogEntry
        """
        return list(self._log)

    @abstractmethod
    def metadata(self) -> MetadataStore:  # pragma: no cover
        """Get metadata that is associated with the current dataset version.

        Returns
        -------
        openclean.data.metadata.base.MetadataStore
        """
        raise NotImplementedError()

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
        func: openclean.engine.object.func.FunctionHandle
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
        df = self.checkout()
        # Create an action object for the update operation.
        action = UpdateOp(
            schema=list(df.columns),
            columns=columns,
            func=func,
            args=args,
            sources=sources
        )
        # Run the update operation and commit the new dataset version.
        df = update(df=df, columns=columns, func=action.to_eval())
        return self.add_snapshot(df=df, action=action)

    def version(self) -> int:
        """Get version identifier for the last snapshot of the dataset.

        Returns
        -------
        int
        """
        return self._log.last_version()


class FullDataset(DatasetHandle):
    """Handle for datasets that are managed by the openclean engine and that
    have their histor being maintained by an archive manager. All operations
    are applied directly on the full dataset in the underlying archive.
    """
    def __init__(
        self, datastore: ArchiveStore, manager: ArchiveManager, identifier: str,
        pk: Optional[Union[List[str], str]] = None
    ):
        """Initialize the reference to the datastore that maintains the history
        of the dataset that is being tranformed.

        Parameters
        ----------
        datastore: openclean.data.archive.base.ArchiveStore
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
            log=OperationLog(snapshots=datastore.snapshots(), auto_commit=True),
            is_sample=False
        )
        self.datastore = datastore
        self.manager = manager
        self.identifier = identifier
        self.pk = pk

    def add_snapshot(self, df: pd.DataFrame, action: OpHandle) -> pd.DataFrame:
        """Add a new snapshot to the history of the dataset. Returns the data
        frame for the snapshot.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame fr the new dataset snapshot.
        action: openclean.engine.action.OpHandle
            Operator that created the dataset snapshot.

        Returns
        -------
        pd.DataFrame
        """
        df = self.datastore.commit(df=df, action=action)
        # Add the operator to the internal log.
        self._log.add(version=self.datastore.last_version(), action=action)
        return df

    def checkout(self) -> pd.DataFrame:
        """Checkout the latest version of the dataset.

        Returns
        -------
        pd.DataFrame
        """
        return self.datastore.checkout()

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        self.manager.delete(self.identifier)

    def metadata(self) -> MetadataStore:
        """Get metadata that is associated with the current dataset version.

        Returns
        -------
        openclean.data.metadata.base.MetadataStore
        """
        return self.datastore.metadata()


class DataSample(DatasetHandle):
    """Handle for datasets that are samples of a larger dataset. Samples datasets
    are entirely maintained in main memory.

    This class maintains a reference to the orginal sample and the to the current
    modified version of the sample. If intermediate versions need to be accessed
    they will be recreated by re-applying the sequence of operations that generated
    them.

    The class also has a reference to the handle for the full dataset.
    """
    def __init__(self, df: pd.DataFrame, original: DatasetHandle):
        """Initialize the reference to the data sample and the handle for the
        original (full) dataset.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame for the dataset sample.
        original: openclean.engine.dataset.DatasetHandle
            Reference to the original dataset for sampled datasets.
        """
        super(DataSample, self).__init__(
            log=OperationLog(snapshots=original.datastore.snapshots(), auto_commit=False),
            is_sample=True
        )
        self.df = df
        self.original = original
        # Reference to the current dataset version.
        self._current_df = df
        # List of metadata stores for all snapshots. The offset counts the
        # number of commited snapshots for which metadata is not maintained.
        self._metadata = [original.metadata()]
        self._offset = len(self._log) - 1

    def add_snapshot(self, df: pd.DataFrame, action: OpHandle) -> pd.DataFrame:
        """Add a new snapshot to the history of the dataset. Replaces the current
        snapshot with the given data frame and adds the operation handle to the
        internal log.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame fr the new dataset snapshot.
        action: openclean.engine.action.OpHandle
            Operator that created the dataset snapshot.

        Returns
        -------
        pd.DataFrame
        """
        self._current_df = df
        self._log.add(version=len(self._log), action=action)
        self._metadata.append(VolatileMetadataStore())
        return df

    def checkout(self) -> pd.DataFrame:
        """Checkout the latest version of the dataset.

        Returns
        -------
        pd.DataFrame
        """
        return self._current_df

    def commit(self):
        """Apply all actions in the current log to the underlying original
        dataset.
        """
        # Apply each action that is stored in the log. Removes the operation
        # from the log if it had been applied succssfully.
        return exec_operations(
            df=self.original.checkout(),
            operations=[op for op in self.log() if not op.is_committed],
            datastore=self.original.datastore
        )

    def drop(self):
        """Delete all resources that are associated with the dataset history."""
        self.original.drop()

    def metadata(self) -> MetadataStore:
        """Get metadata that is associated with the current dataset version.

        Returns
        -------
        openclean.data.metadata.base.MetadataStore
        """
        return self._metadata[-1]

    def rollback(self, identifier: str) -> pd.DataFrame:
        """Rollback to the dataset version that was created by the log entry
        before the entry with the given identifier. That is, we rollback all
        changes that occurred by the identified operation and all following
        ones. This will make the respective snapshot of the previous entry in
        the operation log the new current (head) snapshot for the dataset
        history.

        Rollback is only supported for uncommitted changes. Removes all log
        entries starting from the rollback version.

        Returns the dataframe for the dataset snapshot that is at the new head
        of the dataset history.

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
        """
        operation_log = self.log()
        apply_ops = list()
        for i in range(len(operation_log)):
            op = operation_log[i]
            if op.identifier == identifier:
                if op.is_committed:
                    raise ValueError('can only rollback uncommitted actions')
                # Remove all log entries starting from the rollback position.
                self._log.truncate(i)
                self._metadata = self._metadata[:i - self._offset]
                # Rollback was successful. Make the dataset snapshot at the
                # previous version the new current snapshot.
                self._current_df = exec_operations(df=self.df, operations=apply_ops)
                # Return here to avoid a KeyError at the end of the method.
                return self._current_df
            elif not op.is_committed:
                apply_ops.append(op)
        # Raise a KeyError if no log entry with the given identifier was found.
        raise KeyError("unknown snapshot '{}'".format(identifier))


# -- Helper Methods -----------------------------------------------------------

def exec_operations(
    df: pd.DataFrame, operations: List[OpHandle], datastore: Optional[Datastore] = None
) -> pd.DataFrame:
    """Re-apply a sequence of operators on a given dataframe. If the datastore
    for the original dataset is given the changes will be commited.

    Parameters
    ----------
    df: pd.DataFrame
        Data frame for original datataset snapshot.
    operations: list of openclean.engine.action.OpHandle
        List of operations that are being appiled.
    datastore: openclean.data.archive.base.Datastore, default=None
        Optional datastore for the full dataset.

    Returns
    -------
    pd.DataFrame
    """
    for op in operations:
        # Execute the operation on the latest snapshot of the original
        # dataset and commit the modified snapshot to that dataset.
        action = op.action
        if action.is_insert:
            df = inscol(
                df=df,
                names=action.names,
                pos=action.pos,
                values=action.to_eval()
            )
        elif action.is_update:
            df = update(df=df, columns=action.columns, func=action.to_eval())
        else:
            raise RuntimeError("cannot re-apply '{}' action".format(op.optype))
        # Commit the dataset (only if the datastore for the full dataset is
        # given).
        if datastore is not None:
            df = datastore.commit(df=df, action=action)
            op.is_committed = True
    return df
