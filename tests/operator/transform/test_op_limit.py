# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the limit operator."""

import pandas as pd
import pytest

from openclean.operator.stream.collector import Collector
from openclean.operator.transform.limit import limit, Limit


@pytest.fixture
def dataset():
    """Generate a data frame with two columns and ten rows."""
    return pd.DataFrame(data=[[i, i + 1] for i in range(10)], columns=['A', 'B'])


def test_limit_transformer(dataset):
    """Simple test for using limit operator to reduce number of rows in a data
    frame. Limit uses pandas head() function so we only look at the number of
    rows in the generated result here.
    """
    assert limit(dataset, 5).shape == (5, 2)
    assert limit(dataset, 10).shape == (10, 2)
    assert limit(dataset, 15).shape == (10, 2)


@pytest.mark.parametrize('limit,result', [(5, 5), (10, 10), (15, 10)])
def test_limit_consumer(limit, result, dataset):
    """Test the limit consumer."""
    collector = Collector()
    consumer = Limit(limit).open(dataset.columns).set_consumer(collector)
    assert list(consumer.columns) == list(dataset.columns)
    for rowid, row in dataset.iterrows():
        try:
            consumer.consume(rowid=rowid, row=row)
        except StopIteration:
            break
    rows = collector.close()
    assert len(rows) == result
    assert [r[0] for r in rows] == list(range(result))
