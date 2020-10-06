# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper methods to read data frames from CSV files."""

import csv
import gzip
import pandas as pd

from typing import List, Optional, Tuple

from openclean.data.column import Column
from openclean.data.select import select_clause
from openclean.function.eval.base import EvalFunction


# -- Data stream --------------------------------------------------------------

class DataStream(object):
    """The data stream class is intended for reading and filtering large CSV
    files as data frames. The class iterates over the rows of a given CSV fle
    reader. It allows to filer columns (select) and rows (where) using an
    openclean predicate.

    The class implements the iterrows() function and columns property of the
    pandas DataFrame. Instances of this class can be used as substitues for
    pandas DataFrames for all openclean functions that only make use of the
    iterrows() functions and the columns property for data frame arguments.
    """
    def __init__(
        self, columns: List[Column], reader: csv.reader, csvfile: int,
        column_filter: Optional[List[int]] = None
    ):
        """Initialize the data frmae columns, the CSV reader, and the optional
        column filter.

        Parameters
        ----------
        columns: list of Column
            List of data frame column objects
        reader: csv.reader
            Reader for the data rows in the streamed CSV file.
        csvfile: File object
            Handle for the open CSV file.
        column_filter: list of int, default=None
            List of column indices in the read rows from the CSV reader that
            are included in the output (set by the select() method).
        """
        self.columns = columns
        self.reader = reader
        self.csvfile = csvfile
        self.column_filter = column_filter

    def iterrows(self):
        """Simulate the iterrows() function of a pandas DataFrame as it is used
        in openclean. Returns an iterator that yields pairs of row identifier
        and value list for each rw in the streamed data frame.
        """
        rowid = 0
        while True:
            try:
                values = next(self.reader)
                if self.column_filter is not None:
                    values = [values[i] for i in self.column_filter]
                yield rowid, values
                rowid += 1
            except StopIteration:
                break
        # Close the CSV file when done streaming.
        self.csvfile.close()

    def select(self, *args):
        """Select a given list of columns from the data frame. Columns may
        either be referenced by their index position or their name.

        Returns a new data stream with the column filter set to the columns
        that were in the argument list.
        """
        colnames, colidxs = select_clause(self, list(args))
        return DataStream(
            columns=colnames,
            reader=self.reader,
            csvfile=self.csvfile,
            column_filter=colidxs
        )

    def where(
        self, predicate: EvalFunction, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Filter rows in the data stream that match a given condition. Returns
        a pandas DataFrame containing the filtered rows. Expects an evaluation
        function as row predicate. Allows to limit the number of rows in the
        returned data frame.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to filter rows.
        limit: int, default=None
            Limit the number of rows in the returned data frame.

        Returns
        -------
        pd.DataFrame
        """
        predicate = predicate.prepare(self)
        data = list()
        index = list()
        for rowid, values in self.iterrows():
            if (predicate.eval(values)):
                data.append(values)
                index.append(rowid)
                if limit is not None and len(data) >= limit:
                    break
        return pd.DataFrame(
            data=data,
            columns=self.columns,
            index=index
        )


# -- Load functions -----------------------------------------------------------

def dataset(filename: str) -> pd.DataFrame:
    reader, csvfile = get_reader(filename)
    columns = list()
    for col_name in next(reader):
        cid = len(columns)
        columns.append(Column(colid=cid, name=col_name.strip(), colidx=cid))
    data = list()
    while True:
        try:
            data.append(next(reader))
        except StopIteration:
            break
    csvfile.close()
    return pd.DataFrame(
        data=data,
        columns=columns,
        index=range(len(data))
    )


def get_reader(filename: str) -> Tuple[csv.reader, int]:
    """Get a CSV reader for the given file. Determines the delimiter based on
    the file suffix. If the file ends with '.gz' it is assumed to be compressed
    using gzip.

    Parameters
    ----------
    filename: string
        Path to CSV file on the local file system.

    Returns
    -------
    ps.DataFrame
    """
    if filename.endswith('.tsv') or filename.endswith('.tsv.gz'):
        delimiter = '\t'
    else:
        delimiter = ','
    if filename.endswith('.gz'):
        csvfile = gzip.open(filename, 'rt')
    else:
        csvfile = open(filename, 'r')
    reader = csv.reader(csvfile, delimiter=delimiter)
    return reader, csvfile


def stream(filename: str) -> DataStream:
    """Read a CSV file as a data stream. This is a helper method that is
    intended to read and filter large CSV files.

    Parameters
    ----------
    filename: string
        Path to CSV file on the local file system.

    Returns
    -------
    openclean.data.load.DataStream
    """
    reader, csvfile = get_reader(filename)
    columns = list()
    for col_name in next(reader):
        cid = len(columns)
        columns.append(Column(colid=cid, name=col_name.strip(), colidx=cid))
    return DataStream(columns=columns, reader=reader, csvfile=csvfile)
