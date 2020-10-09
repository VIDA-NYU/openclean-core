# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes to read data frames from CSV files."""

from typing import List, Optional

import csv
import gzip

from openclean.data.column import ColumnName
from openclean.data.stream.base import DatasetIterator, DatasetStream


class CSVReader(DatasetIterator):
    """Iterable reader for rows in a CSV file. Instances of this class act as a
    context manager for an open CSV file reader.
    """
    def __init__(self, reader: csv.reader, file: int, skip_header: bool):
        """Initialize the reader and the file handle.

        Parameters
        ----------
        reader: csv.reader
            Reader for the data rows in the streamed CSV file.
        file: File object
            Handle for the open CSV file.
        skip_header: bool
            Skip first row if True.
        """
        self.reader = reader
        self.file = file
        # Initialize the rowid counter and read the first line from the file
        # if no list of columns is passed to the object at instantiation.
        self.rowid = 0
        # Read the first row if the skip_header flag is True.
        if skip_header:
            next(reader)

    def __enter__(self):
        """Enter method for the context manager."""
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


class CSVFile(DatasetStream):
    """Helper class to open a CSV reader for a given data file."""
    def __init__(
        self, filename: str, header: Optional[List[ColumnName]] = None,
        delim: Optional[str] = None, compressed: Optional[bool] = None
    ):
        """Set the file name, delimiter and the flag that indicates if the file
        is compressed using gzip.

        Parameters
        ----------
        filename: string
            Path to the CSV file that is being read.
        header: list of string, default=None
            Optional header. If no header is given it is assumed that the first
            row in the CSV file contains the header information.
        delim: string, default=None
            The column delimiter used in the CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents have been compressed using
            gzip.
        """
        self.filename = filename
        # Keep track if the header information was None intially, i.e., if the
        # header is in the file or not.
        self._has_header = header is None
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
        # Read the header information from the first line of the input file if
        # no header is given.
        if header is None:
            reader, csvfile = self._openreader()
            try:
                header = next(reader)
            finally:
                csvfile.close()
        super(CSVFile, self).__init__(columns=header)

    def open(self) -> CSVReader:
        """Get a CSV reader for the associated CSV file.

        Returns
        -------
        openclean.data.stream.csv.CSVReader
        """
        reader, csvfile = self._openreader()
        return CSVReader(
            reader=reader,
            file=csvfile,
            skip_header=self._has_header
        )

    def _openreader(self):
        """Open the associated CSV file. Returns a file handle for the opened
        file.

        Returns
        -------
        csv.reader, file object
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'rt')
        else:
            csvfile = open(self.filename, 'r')
        return csv.reader(csvfile, delimiter=self.delim), csvfile

    def write(self) -> CSVWriter:
        """Get a CSV writer for the associated CSV file. Writes the dataset
        header information to the opened output file.

        Returns
        -------
        openclean.data.load.CSVWriter
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'wt', newline='')
        else:
            csvfile = open(self.filename, 'w', newline='')
        # Open te CSV writer and output the dataset header.
        writer = csv.writer(csvfile, delimiter=self.delim)
        writer.writerow(self.columns)
        return CSVWriter(writer=writer, file=csvfile)
