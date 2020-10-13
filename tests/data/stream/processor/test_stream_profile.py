# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the limit operator in data processing pipeline."""

from openclean.profiling.tests import ValueCounter


def test_profile_all_columns(stream311):
    """Test profiling all columns in a given data stream."""
    metadata = stream311.profile()
    assert len(metadata) == 4


def test_profile_one_columns(stream311):
    """Test profiling a single column in a given data stream."""
    metadata = stream311.profile(profilers=0)
    assert len(metadata) == 1
    metadata = stream311.profile(profilers=(0, ValueCounter()))
    assert len(metadata) == 1


def test_profile_two_columns(stream311):
    """Test profiling all columns in a given data stream."""
    metadata = stream311.profile(profilers=[0, (2, ValueCounter())])
    assert len(metadata) == 2
