# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes to read data frames from CSV files."""

import csv
import gzip

from typing import Optional


class CSVReader(object):
    """Iterable reader for rows in a CSV file. This is a context manager for an
    open CSV file reader.
    """
    def __init__(self, reader, file):
        """Initialize the reader and the file handle.

        Parameters
        ----------
        reader: csv.reader
            Reader for the data rows in the streamed CSV file.
        file: File object
            Handle for the open CSV file.
        """
        self.reader = reader
        self.file = file
        self.rowid = None

    def __enter__(self):
        """Enter method for the context manager."""
        self.rowid = 0
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.file.close()
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from CSV reader. The CSV reader will raise a
        StopIteration error when the end of the file is reached.
        """
        row = next(self.reader)
        rowid = self.rowid
        self.rowid += 1
        return rowid, row

    def header(self):
        """Return the next row in the CSV file."""
        return next(self.reader)


class CSVWriter(object):
    """Context manager for an open CSV file writer."""
    def __init__(self, writer, file):
        """Initialize the writer and the file handle.

        Parameters
        ----------
        writer: csv.writer
            Writer for a streamed CSV file.
        file: File object
            Handle for the open CSV file.
        """
        self.writer = writer
        self.file = file

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.close()
        return False

    def close(self):
        """Close the associated file handle and set it to None (to avoid
        repeated attempts to close an already closed file).
        """
        if self.file is not None:
            self.file.close()
            self.file = None

    def write(self, row):
        """Write a row to the CSV file."""
        return self.writer.writerow(row)


class CSVFile(object):
    """Helper class to open a CSV reader for a given data file."""
    def __init__(
        self, filename: str, delim: Optional[str] = None,
        compressed: Optional[bool] = None
    ):
        """Set the file name, delimiter and the flag that indicates if the file
        is compressed using gzip.

        Parameters
        ----------
        filename: string
            Path to the CSV file that is being read.
        delim: string, default=None
            The column delimiter used in the CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents have been compressed using
            gzip.
        """
        self.filename = filename
        # Infer the delimiter from the filename if not given. Files that have
        # '.tsv' in their name are expscted to be tab-dlimited. All other files
        # are expected to be standard CSV files. In the future we may want to
        # use a sniffer here.
        if delim is None:
            if filename.endswith('.tsv') or filename.endswith('.tsv.gz'):
                delim = '\t'
            else:
                delim = ','
        self.delim = delim
        # Infer compression if gzip is not set. Files that end with '.gz' are
        # assumed to be compressed using gzip.
        if compressed is None:
            compressed = filename.endswith('.gz')
        self.compressed = compressed

    def open(self, skip_header: Optional[bool] = False) -> CSVReader:
        """Get a CSV reader for the associated CSV file. If the skip_header
        flag is True the first row of the file will be ignored.

        Parameters
        ----------
        skip_header: bool
            Ignore first row of the opened CSV file if True.

        Returns
        -------
        openclean.data.load.CSVReader
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'rt')
        else:
            csvfile = open(self.filename, 'r')
        reader = csv.reader(csvfile, delimiter=self.delim)
        # Skip first row if skip_header flag is True.
        if skip_header:
            next(reader)
        return CSVReader(reader=reader, file=csvfile)

    def write(self) -> CSVWriter:
        """Get a CSV writer for the associated CSV file.

        Returns
        -------
        openclean.data.load.CSVWriter
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'wb', newline='')
        else:
            csvfile = open(self.filename, 'w', newline='')
        writer = csv.writer(csvfile, delimiter=self.delim)
        return CSVWriter(writer=writer, file=csvfile)
