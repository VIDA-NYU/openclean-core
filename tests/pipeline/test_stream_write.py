# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for writing the rows in the data processing pipeline to file."""

import os
import pandas as pd

from openclean.pipeline import stream


def test_write_stream(tmpdir):
    """Test writing a data stream to file."""
    # Create a data stream with three rows.
    data = [[1, 2, 3], [3, 4, 5], [5, 6, 7]]
    df = pd.DataFrame(data=data, columns=['A', 'B', 'C'])
    # Write data stream to file.
    filename = os.path.join(tmpdir, 'myfile.csv')
    stream(df).write(filename)
    # Read the created file.
    data = list()
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip())
    assert data == ['A,B,C', '1,2,3', '3,4,5', '5,6,7']
