# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Limit consumer for data streams."""

import pytest

from openclean.pipeline.consumer.collector import Collector
from openclean.pipeline.consumer.producer import Limit


def test_stop_error():
    """Ensure that an error is raised after the limit of rows is reached."""
    consumer = Limit(limit=10, consumer=Collector())
    for i in range(10):
        consumer.consume(i, [i])
    with pytest.raises(StopIteration):
        consumer.consume(10, [10])


@pytest.mark.parametrize('count', [0, 5, 10])
def test_vaid_limits(count):
    """Test passing less (or at most the maximum) number of rows to the
    Limit consumer.
    """
    collector = Collector()
    consumer = Limit(limit=10, consumer=collector)
    for i in range(count):
        consumer.consume(i, [i])
    assert len(collector.rows) == count
    assert [rowid for rowid, _ in collector.rows] == list(range(count))
