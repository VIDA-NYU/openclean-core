# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Limit consumer for data streams."""


from openclean.data.stream.consumer import Profile
from openclean.profiling.tests import ValueCounter


def test_profile_columns():
    """Test profiling two columns over a short data stream."""
    consumer = Profile(
        profilers=[(0, 'A', ValueCounter()), (2, 'B', ValueCounter())]
    )
    consumer.consume(0, [1, 2, 3])
    consumer.consume(1, [1, 5, 6])
    results = consumer.close()
    assert len(results) == 2
    assert results[0] == {'column': 'A', 'stats': {1: 2}}
    assert results[1] == {'column': 'B', 'stats': {3: 1, 6: 1}}
