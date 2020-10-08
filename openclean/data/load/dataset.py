# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper methods to create a pandas DataFrame from the data in
a given CSV file.
"""

import pandas as pd

from typing import Optional

from openclean.data.column import Column
from openclean.data.load.csv import CSVFile


def dataset(
    filename: str, delim: Optional[str] = None,
    compressed: Optional[bool] = None
) -> pd.DataFrame:
    """Read a pandas data frame from a CSV file. This function infers the
    CSV file delimiter and compression from the file name (if not specified).
    By now the inference follows a very basic pattern. Files that have '.tsv'
    (or '.tsv.gz') as their suffix are expected to be tab-delimited. Files that
    end with '.gz' are expected to be gzip compressed.

    Returns a pandas DataFrame where the column names are instances of the
    identifiable Column class used by openclean.

    Parameters
    ----------
    filename: string
        Path to the CSV file that is being read.
    delim: string, default=None
        The column delimiter used in the CSV file.
    compressed: bool, default=None
        Flag indicating if the file contents have been compressed using
        gzip.

    Returns
    -------
    pd.DataFrame
    """
    file = CSVFile(filename=filename, delim=delim, compressed=compressed)
    with file.open(skip_header=False) as reader:
        columns = list()
        for col_name in reader.header():
            cid = len(columns)
            col = Column(colid=cid, name=col_name.strip(), colidx=cid)
            columns.append(col)
        data = list()
        index = list()
        for rowid, row in reader:
            data.append(row)
            index.append(rowid)
        return pd.DataFrame(data=data, columns=columns, index=index)
