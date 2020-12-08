# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for majority vote selector."""

import pytest

from openclean.function.value.aggregate import Max
from openclean.function.value.vote import MajorityVote


def test_vote_without_tie():
    """Test majority vote selection from a list with no ties."""
    # Get constant function that will always return he most frequent value (i.e.,
    # 3) in the given list.
    f = MajorityVote().prepare([1, 2, 2, 3, 3, 3])
    assert f(0) == 3
    assert f(1) == 3


def test_majority_vote_with_tie():
    """Test tiebraking for lists that have multiple most frequent values."""
    # Error when no tiebreaker is given.
    with pytest.raises(ValueError):
        MajorityVote().prepare([1, 2, 2, 3, 3])
    # Break ties between 2 and 3 using Max.
    f = MajorityVote(tiebreaker=Max()).prepare([1, 2, 2, 3, 3])
    assert f(0) == 3
    assert f(1) == 3
