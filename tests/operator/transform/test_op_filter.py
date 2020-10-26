# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for filter operator."""

from openclean.data.sequence import Sequence
from openclean.function.eval.base import Col
from openclean.function.eval.domain import IsIn
from openclean.operator.stream.collector import Collector
from openclean.operator.transform.filter import delete, filter, Filter


def test_delete_consumer():
    """Test filtering rows from a data stream."""
    collector = Collector()
    pred = Col(column='A', colidx=0) > 3
    consumer = Filter(predicate=pred, negated=True)\
        .open(['A', 'B', 'C'])\
        .set_consumer(collector)
    assert consumer.columns == ['A', 'B', 'C']
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 1
    assert rows[0] == (3, [1, 2, 3])


def test_delete_operator(dupcols):
    """Test delete rows in a data frame."""
    assert 'Alice' in Sequence(dupcols, 'Name')
    df = delete(dupcols, Col('Name') == 'Alice')
    assert df.shape == (6, 3)
    assert 'Alice' not in Sequence(df, 'Name')


def test_filter_operator(agencies):
    """Test filtering rows in data frame."""
    # There are two rows with empty values in attribute Age
    d1 = filter(
        agencies,
        IsIn(
            columns=['borough', 'state'],
            domain=[('bk', 'Ny'), ('Bx', 'NY')],
            ignore_case=True
        )
    )
    assert d1.shape == (7, 3)
    d1 = filter(agencies, Col('agency') > Col('state'))
    assert d1.shape == (2, 3)


def test_filter_consumer():
    """Test filtering rows from a data stream."""
    collector = Collector()
    pred = Col(column='A', colidx=0) > 3
    consumer = Filter(predicate=pred)\
        .open(['A', 'B', 'C'])\
        .set_consumer(collector)
    assert consumer.columns == ['A', 'B', 'C']
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 2
    assert rows[0] == (2, [4, 5, 6])
    assert rows[1] == (1, [7, 8, 9])
