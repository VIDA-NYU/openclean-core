# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the mapping operator."""

from openclean.function.value.mapping import mapping


def test_simple_mapping(employees):
    """Test creating a mapping over a single column."""
    names = mapping(employees, 'Name', str.upper)
    assert len(names) == 7
    for key, value in names.items():
        assert key.upper() == value
