# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Log of actions that defines the history of a dataset."""

from dataclasses import dataclass
from typing import Dict, List, Optional

from openclean.data.archive.base import Snapshot
from openclean.engine.action import OpHandle


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
    # Action that created the snapshot (only set for uncommitted operations).
    action: Optional[OpHandle] = None
    # Version identifier for snapshot in a dataset sample (not given for
    # committed snapshots).
    version: Optional[int] = None


class OperationLog(object):
    """The operation log maintains a list of entries containing provenance
    information for each snapshot of a dataset. Snapshots in a dataset can either
    be committed, i.e., persisted with the datastore that manages the full dataset,
    or uncommitted, i.e., committed only with the datastore for a dataset sample but
    not the full dataset.
    """
    def __init__(self, snapshots: List[Snapshot]):
        """Initialize the list of committed snapshots.

        Parameters
        ----------
        snapshots: list of histore.archive.snapshot.Snapshot
            List of committe snapshots from a dataset.
        """
        self.entries = [LogEntry(descriptor=s.action, version=s.version) for s in snapshots]

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
