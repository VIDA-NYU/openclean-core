# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the dataset log."""

import pytest

from histore.archive.snapshot import SnapshotListing
from openclean.engine.action import InsertOp, OP_INSCOL
from openclean.engine.log import LogEntry, OperationLog


def test_log_entry_committed():
    """Test the is_committed property of the log entry."""
    # Cannot set the is_committed property of a committed entry to False.
    e = LogEntry(descriptor=dict())
    assert e.is_committed
    e.is_committed = True
    with pytest.raises(ValueError):
        e.is_committed = False
    # Can set the is_commited property for an uncommited entry to False (once).
    e = LogEntry(descriptor=dict(), version=0, action=InsertOp(schema=[], names='A'))
    assert not e.is_committed
    e.is_committed = False
    assert not e.is_committed
    e.is_committed = True
    assert e.is_committed
    with pytest.raises(ValueError):
        e.is_committed = False


def test_log_entry_identifier():
    """Ensure that log entries are assigned a unique identifier by default."""
    e1 = LogEntry(descriptor=dict())
    e2 = LogEntry(descriptor=dict())
    assert e1.identifier is not None
    assert e2.identifier is not None
    assert e1.identifier != e2.identifier


def test_log_rollback():
    """Test rollback for log operations."""
    log = OperationLog(snapshots=list(), auto_commit=False)
    for i in range(5):
        log.add(version=i, action=InsertOp(schema=[], names='A', pos=i))
    assert len(log) == 5
    log.truncate(pos=3)
    assert len(log) == 3
    positions = [op.descriptor['pos'] for op in log]
    assert positions == [0, 1, 2]
    versions = [op.version for op in log]
    assert versions == [0, 1, 2]


def test_operation_log_auto_commit():
    """Test functions of the operations log with auto_commit set to True."""
    snapshots = SnapshotListing()
    snapshots = snapshots.append(version=0, action={'action': 0})
    log = OperationLog(snapshots=snapshots, auto_commit=True)
    assert len(log) == 1
    log.add(version=1, action=InsertOp(schema=[], names=['A', 'B'], pos=1))
    assert len(log) == 2
    for op in log:
        assert op.is_committed
    descriptors = [op.descriptor for op in log]
    assert descriptors[0] == {'action': 0}
    assert descriptors[1] == {'optype': OP_INSCOL, 'columns': ['A', 'B'], 'pos': 1}


def test_operation_log_uncommitted():
    """Test functions of the operations log with auto_commit set to False."""
    snapshots = SnapshotListing()
    snapshots = snapshots.append(version=0, action={'action': 0})
    log = OperationLog(snapshots=snapshots, auto_commit=False)
    assert len(log) == 1
    log.add(version=1, action=InsertOp(schema=[], names=['A', 'B'], pos=1))
    assert len(log) == 2
    operators = [op for op in log]
    assert operators[0].is_committed
    assert not operators[1].is_committed
    descriptors = [op.descriptor for op in log]
    assert descriptors[0] == {'action': 0}
    assert descriptors[1] == {'optype': OP_INSCOL, 'columns': ['A', 'B'], 'pos': 1}
