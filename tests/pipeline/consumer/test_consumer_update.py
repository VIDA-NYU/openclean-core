# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Update consumer for data streams."""

from openclean.function.eval.base import Col, Const
from openclean.pipeline.consumer.collector import Collector
from openclean.pipeline.consumer.producer import Update


def test_ternary_update_consumer():
    """Test updating a multiple columns in a data stream."""
    collector = Collector()
    func = Const([0, 1])
    consumer = Update(columns=[2, 1], func=func, consumer=collector)
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 3
    assert rows[0] == (3, [1, 1, 0])
    assert rows[1] == (2, [4, 1, 0])
    assert rows[2] == (1, [7, 1, 0])


def test_unary_update_consumer():
    """Test updating a single column in a data stream."""
    collector = Collector()
    func = Col(columns='A', colidx=0)
    consumer = Update(columns=[1], func=func, consumer=collector)
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 3
    assert rows[0] == (3, [1, 1, 3])
    assert rows[1] == (2, [4, 4, 6])
    assert rows[2] == (1, [7, 7, 9])
