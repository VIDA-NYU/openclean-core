# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for writing the rows in the data processing pipeline to file."""

import os


def test_write_stream(stream311, tmpdir):
    """Test writing row in a stream to file."""
    filename = os.path.join(tmpdir, 'myfile.csv')
    # Write first ten rows to file.
    stream311.limit(10).write(filename)
    # Read file.
    data = list()
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip())
    assert len(data) == 11
    assert data[0] == 'descriptor,borough,city,street'
    assert data[1] != 'descriptor,borough,city,street'
