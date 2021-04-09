# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the stream archive writer."""

from histore.document.schema import to_schema
from histore.document.stream import DocumentConsumer


# -- Helper classes -----------------------------------------------------------

class RowCollector(DocumentConsumer):
    def __init__(self):
        self.rows = list()

    def consume_document_row(self, row, version):
        self.rows.append((row.key.value, row.pos, row.values, version))


def test_load_stream(ds):
    """Test counting the number of rows in a stream sample."""
    collector = RowCollector()
    ds.stream_to_archive(schema=to_schema(ds.columns), version=0, consumer=collector)
    assert collector.rows == [
        (0, 0, {0: 'A', 1: 0, 2: 9}, 0),
        (1, 1, {0: 'A', 1: 1, 2: 8}, 0),
        (2, 2, {0: 'A', 1: 2, 2: 7}, 0),
        (3, 3, {0: 'A', 1: 3, 2: 6}, 0),
        (4, 4, {0: 'A', 1: 4, 2: 5}, 0),
        (5, 5, {0: 'A', 1: 5, 2: 4}, 0),
        (6, 6, {0: 'A', 1: 6, 2: 3}, 0),
        (7, 7, {0: 'A', 1: 7, 2: 2}, 0),
        (8, 8, {0: 'A', 1: 8, 2: 1}, 0),
        (9, 9, {0: 'A', 1: 9, 2: 0}, 0)
    ]
