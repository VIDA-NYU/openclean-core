# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the parallel processing engine."""

from openclean.engine.parallel import process_list


def add_one(x):
    """Helper function."""
    return x + 1


def test_parallel_process_list():
    """Test paralle processing of a list of values."""
    values = process_list(func=add_one, values=[2, 4, 6, 8], processes=2)
    # Order is not guaranteed.
    assert set(values) == {3, 5, 7, 9}
