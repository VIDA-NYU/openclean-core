# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Write consumer for data streams."""

import os

from openclean.data.stream.csv import CSVFile
from openclean.operator.stream.collector import Write


def test_write_rows(tmpdir):
    """Test writing rows to a CSV files."""
    filename = os.path.join(tmpdir, 'out.csv')
    consumer = Write(file=CSVFile(filename, header=['A', 'B', 'C']))\
        .open(['A', 'B', 'C'])
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    consumer.close()
    data = list()
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip())
    assert data == [
        'A,B,C',
        '1,2,3',
        '4,5,6',
        '7,8,9'
    ]
