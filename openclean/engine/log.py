# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Log of actions that defines the history of a dataset."""

from dataclasses import dataclass, field
from histore.archive.snapshot import Snapshot
from typing import Dict, List, Optional

from openclean.engine.action import OpHandle

import openclean.util.core as util


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

    def last_version(self) -> int:
        """Get version identifier of the last entry in the log.

        Returns
        -------
        int
        """
        return self.entries[-1].version

    def truncate(self, pos: int):
        """Remove all log entries starting at the given index.

        Parameters
        ----------
        pos: int
            List position from which (including the position) all entries in
            the log are removed.
        """
        self.entries = self.entries[:pos]
