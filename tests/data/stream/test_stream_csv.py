# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for CSV file streams."""

import os
import pytest

from openclean.data.stream.csv import CSVFile


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
NYC311_FILE = os.path.join(DIR, '311-descriptor.csv')
ICT_FILE = os.path.join(DIR, 'ICT-development-index.tsv.gz')


@pytest.mark.parametrize(
    'filename,header,rows',
    [
        (NYC311_FILE, ['descriptor', 'borough', 'city', 'street'], 303),
        (ICT_FILE, ['year', 'country_id', 'country_name', 'sub_index', 'value_type', 'value'], 2663)  # noqa: E501
    ]
)
def test_stream_existing_file(filename, header, rows):
    """Test streaming exising plain CSV and tab-delimited, compressed CSV
    files.
    """
    dataset = CSVFile(filename)
    assert dataset.columns == header
    for rowid, row in dataset.iterrows():
        assert len(row) == len(header)
    assert rowid == rows


@pytest.mark.parametrize('compressed', [True, False])
def test_read_file_without_header(compressed, tmpdir):
    """Read a CSV file that does not have a header."""
    # -- Setup ----------------------------------------------------------------
    # Write rows (without header) for 311 data file to a temporary text file.
    tmpfile = os.path.join(tmpdir, 'myfile.txt')
    header = ['A', 'B', 'C', 'D']
    file = CSVFile(tmpfile, header=header, compressed=compressed)
    write_count = 0
    with file.write() as writer:
        with CSVFile(NYC311_FILE).open() as f:
            for _, row in f:
                writer.write(row)
                write_count += 1
    # -- Read the created CSV file with user-provided header ------------------
    file = CSVFile(tmpfile, compressed=compressed)
    assert file.columns == header
    read_count = 0
    with file.open() as f:
        for rowid, row in f:
            assert len(row) == len(header)
            read_count += 1
    assert read_count == write_count


def test_none_converter(tmpdir):
    """Test converting encodings of None to None when reading and writing a
    CSV file.
    """
    # Create a CSV file with three rows where None is encoded as n.
    tmpfile = os.path.join(tmpdir, 'myfile.txt')
    with open(tmpfile, 'w') as f:
        f.write('A,B\n')
        f.write('1,n\n')
        f.write('n,1\n')
        f.write('n,n\n')
    # Read the file to ensure that 'n' is replaced by None
    file = CSVFile(tmpfile, none_is='n')
    with file.open() as f:
        rows = [r for _, r in f]
    assert rows == [['1', None], [None, '1'], [None, None]]
    # Write the file and replace None with '-'
    with file.write(header=['A', 'B'], none_as='-') as f:
        for row in rows:
            f.write(row)
    with open(tmpfile, 'r') as f:
        lines = [line.strip() for line in f]
    assert lines == ['A,B', '1,-', '-,1', '-,-']
