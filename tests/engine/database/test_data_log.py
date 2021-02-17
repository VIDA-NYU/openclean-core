# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the dataset log."""

import pytest

from histore.archive.snapshot import SnapshotListing
from openclean.engine.action import InsertOp, OP_INSCOL
from openclean.engine.log import LogEntry, OperationLog


def test_log_rollback():
    """Test rollback for log operations."""
    log = OperationLog(snapshots=list())
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
    log = OperationLog(snapshots=snapshots)
    assert len(log) == 1
    log.add(version=1, action=InsertOp(schema=[], names=['A', 'B'], pos=1))
    assert len(log) == 2
    descriptors = [op.descriptor for op in log]
    assert descriptors[0] == {'action': 0}
    assert descriptors[1] == {'optype': OP_INSCOL, 'columns': ['A', 'B'], 'pos': 1}


def test_operation_log_uncommitted():
    """Test functions of the operations log with auto_commit set to False."""
    snapshots = SnapshotListing()
    snapshots = snapshots.append(version=0, action={'action': 0})
    log = OperationLog(snapshots=snapshots)
    assert len(log) == 1
    log.add(version=1, action=InsertOp(schema=[], names=['A', 'B'], pos=1))
    assert len(log) == 2
    descriptors = [op.descriptor for op in log]
    assert descriptors[0] == {'action': 0}
    assert descriptors[1] == {'optype': OP_INSCOL, 'columns': ['A', 'B'], 'pos': 1}
