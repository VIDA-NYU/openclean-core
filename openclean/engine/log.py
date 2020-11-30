# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handles for operators that have been applied to a dataset. Handles are stored
in a log (provenance record) for the dataset. The operator handles contain all
the information that is necessary to reapply the opertor to a dataset version.
"""

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from histore.archive.snapshot import Snapshot
from typing import Dict, List, Optional, Union

from openclean.data.types import Columns, Scalar
from openclean.function.eval.base import Eval, EvalFunction, to_const_eval
from openclean.engine.library.func import FunctionHandle

import openclean.util.core as util


# -- Opration handles ---------------------------------------------------------

class OpHandle(metaclass=ABCMeta):
    """The operator handle defines the interface for entries in the provenance
    log of a dataset. The defined methods are used to store the handle and to
    re-apply the operation using a evaluation function that is generated from
    the operator metadata.
    """
    def __init__(
        self, optype: str, columns: Columns,
        func: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the operator metadata.

        Parameters
        ----------
        optype: string
            Unique operator type identifier.
        columns: int, string, or list of int or string
            List of columns. This defines the columns that are affected (e.g.,
            updated) by the operation.
        func: scalar or openclean.engine.library.func.FunctionHandle
            Scalar value or library function that is used to generate values for
            the modified column(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.
        """
        self.optype = optype
        self.columns = columns
        self.func = func
        self.args = args
        self.sources = sources

    @property
    def is_insert(self) -> bool:
        """True if the operator type is 'insert'.

        Returns
        -------
        bool
        """
        return self.optype == 'insert'

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        doc = {'type': self.optype, 'columns': self.columns}
        if self.func is not None:
            if isinstance(self.func, FunctionHandle):
                doc['func'] = self.func.to_descriptor()
            else:
                # Assume that func is a scalar value.
                doc['func'] = self.func
        if self.args is not None:
            doc['arguments'] = self.args
        if self.sources is not None:
            doc['sources'] = self.sources
        return doc

    @abstractmethod
    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise NotImplementedError()


class InsertOp(OpHandle):
    """Handle for an insert operation."""
    def __init__(
        self, names: Union[str, List[str]], pos: Optional[int] = None,
        values: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an insert operator.

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
        """
        super(InsertOp, self).__init__(
            optype='insert',
            columns=names,
            func=values,
            args=args,
            sources=sources
        )
        self.pos = pos

    @property
    def names(self) -> Union[str, List[str]]:
        """Synonym for accessing the columns (which are the names of the inserted
        columns for an inscol operator).

        Returns
        -------
        string or list of string
        """
        return self.columns

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        doc = super(InsertOp, self).to_dict()
        doc['pos'] = self.pos
        return doc

    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        # Return nothing if the associated function is None.
        if self.func is None:
            # Return nothing if the associated function is None.
            return None
            # Use the update columns as input columns if no source columns are
            # specified explicitly.
        if not isinstance(self.func, FunctionHandle):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return to_const_eval(self.func)
        # Return an evaluation function that wraps the callable. The input for
        # that function comes from the list of sources.
        return Eval(columns=self.sources, func=self.func, args=self.args)


class UpdateOp(OpHandle):
    """Handle for an update operation."""
    def __init__(
        self, columns: Columns, func: FunctionHandle, args: Optional[Dict] = None,
        sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an update operator.

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
        """
        super(UpdateOp, self).__init__(
            optype='update',
            columns=columns,
            func=func,
            args=args,
            sources=sources
        )

    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        if self.func is None:
            # Return nothing if the associated function is None.
            return None
            # Use the update columns as input columns if no source columns are
            # specified explicitly.
        if not isinstance(self.func, FunctionHandle):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return to_const_eval(self.func)
        # Create an evaluation function that wraps the callable. The input
        # columns depend on whether sources are given or not.
        columns = self.sources if self.sources is not None else self.columns
        return Eval(columns=columns, func=self.func, args=self.args)


# -- Operation log ------------------------------------------------------------

@dataclass
class LogEntry:
    """Entry in an operation log for a dataset. Each entry maintains information
    about a committed or uncommitted snapshot of a dataset. Each log entry is
    associated with a unique UUID identifer and a descriptor for the action that
    created the snapshot.

    For uncommitted snapshots the handle for the action that created the snapshot
    is maintained together with the version identifier in the data store for the
    dataset sample.
    """
    # Descriptor for the operation that created a snapshot (used for display).
    descriptor: Dict
    # Unique identifier.
    identifier: str = field(default_factory=util.unique_identifier)
    # Action that created the snapshot (only set for uncommitted operations).
    action: Optional[OpHandle] = None
    # Version identifier for snapshot in a dataset sample (not given for
    # committed snapshots).
    version: Optional[int] = None

    @property
    def is_committed(self) -> bool:
        """True, if the snapshot has been committed with the datastore that manages
        the full dataset. False, if the snapshot has only be committed with the
        datastore that manages the data sample. Only uncommitted snapshots have
        the operation handle associated with it. This information is used by the
        `is_committed` property.

        Returns
        -------
        bool
        """
        return self.action is None

    @is_committed.setter
    def is_committed(self, value: bool):
        """Set the committed flag. It is only possible to set the flag to True.
        An attempt to set the flag to False will raise a ValueError.

        Raises
        ------
        ValueError
        """
        if not value and self.action is None:
            raise ValueError('cannot undo operation commit')
        elif value:
            self.action = None


class OperationLog(object):
    """The operation log maintains a list of entries containing provenance
    information for each snapshot of a dataset. Snapshots in a dataset can either
    be committed, i.e., persisted with the datastore that manages the full dataset,
    or uncommitted, i.e., committed only with the datastore for a dataset sample but
    not the full dataset.
    """
    def __init__(self, snapshots: List[Snapshot], auto_commit: bool):
        """Initialize the list of committed snapshots.

        Parameters
        ----------
        snapshots: list of histore.archive.snapshot.Snapshot
            List of committe snapshots from a dataset.
        auto_commit: bool
            Flag indicating whether the dataset handle with which this log is
            associated operates on the full dataset (auto_commit=True) or on a
            dataset sample (auto_commit=False).
        """
        self.entries = [LogEntry(descriptor=s.action) for s in snapshots]
        self.auto_commit = auto_commit

    def __iter__(self):
        """Return an iterator over entries in the log."""
        return iter(self.entries)

    def __len__(self):
        """Get number of entries in the log.

        Returns
        -------
        int
        """
        return len(self.entries)

    def add(self, version: int, action: OpHandle):
        """Append a record to the log.

        Parameters
        ----------
        version: int
            Dataset snapshot version identifier.
        action: openclean.engine.log.OpHandle
            Handle for the operation that created the dataset snapshot.
        """
        if self.auto_commit:
            # Add only the action descriptor for snaphsots that have been
            # committed.
            entry = LogEntry(descriptor=action.to_dict())
        else:
            # Include the action and version identifier for uncommitted snapshots.
            entry = LogEntry(
                action=action,
                descriptor=action.to_dict(),
                version=version
            )
        self.entries.append(entry)

    def rollback(self, pos: int, version: int, action: OpHandle):
        """Remove all log entries starting at the given index Append a new log
        entry to the truncated list for the given version and action.

        Parameters
        ----------
        pos: int
            List position from which (including the position) all entries in
            the log are removed.
        version: int
            Dataset snapshot version identifier.
        action: openclean.engine.log.OpHandle
            Handle for the operation that created the dataset snapshot identified
            by the version.
        """
        self.entries = self.entries[:pos]
        self.add(version=version, action=action)
