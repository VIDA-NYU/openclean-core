# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the stream iterator."""

from openclean.function.eval.base import Col


def test_limit_operator(ds):
    """Test iterating over a stream with a limit operator."""
    with ds.limit(3).open() as reader:
        rows = [row for row in reader]
    assert rows == [
        (0, 0, ['A', 0, 9]),
        (1, 1, ['A', 1, 8]),
        (2, 2, ['A', 2, 7]),
    ]


def test_modified_sample_stream(ds):
    """Test iterating over a sample from the stream where the samples rows
    are further manipulated by downstream operators.
    """
    stream = ds\
        .sample(n=3, random_state=42)\
        .update('A', Col('B') + Col('C'))\
        .select(['A', 'B'])\
        .filter(Col('B') != 5)\
        .limit(1)
    with stream.open() as reader:
        rows = [row for row in reader]
    assert rows == [(0, 9, [9, 9])]


def test_sample_stream(ds):
    """Test iterating over a sample from the stream."""
    with ds.sample(n=3, random_state=42).open() as reader:
        rows = [row for row in reader]
    assert rows == [(0, 5, ['A', 5, 4]), (1, 9, ['A', 9, 0]), (2, 2, ['A', 2, 7])]


def test_stream_without_operators(ds):
    """Test streaming rows from a pipeline without operators."""
    with ds.open() as reader:
        rows = [row for row in reader]
    assert rows == [
        (0, 0, ['A', 0, 9]),
        (1, 1, ['A', 1, 8]),
        (2, 2, ['A', 2, 7]),
        (3, 3, ['A', 3, 6]),
        (4, 4, ['A', 4, 5]),
        (5, 5, ['A', 5, 4]),
        (6, 6, ['A', 6, 3]),
        (7, 7, ['A', 7, 2]),
        (8, 8, ['A', 8, 1]),
        (9, 9, ['A', 9, 0])
    ]


def test_update_and_filter(ds):
    """Test updating and filtering rows."""
    stream = ds\
        .update('A', Col('B') + Col('C'))\
        .select(['A', 'B'])\
        .filter(Col('B') < 5)
    with stream.open() as reader:
        rows = [row for row in reader]
    assert rows == [
        (0, 0, [9, 0]),
        (1, 1, [9, 1]),
        (2, 2, [9, 2]),
        (3, 3, [9, 3]),
        (4, 4, [9, 4]),
    ]
