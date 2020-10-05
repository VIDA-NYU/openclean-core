# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for unique column combination base classes."""

import pytest

from openclean.profiling.constraints.ucc import UniqueColumnSet


def test_init_unique_column_sets():
    """Test initialization of unique column sets."""
    assert UniqueColumnSet(columns=['A', 'B', 'C']) == {'A', 'B', 'C'}
    assert UniqueColumnSet(columns=['A', 'B', 'B']) == {'A', 'B'}
    with pytest.raises(ValueError):
        UniqueColumnSet(columns=['A', 'B', 'B'], duplicate_ok=False)
