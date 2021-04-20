# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper methods to load a dataset from CSV files."""

import pandas as pd

from typing import Optional

from openclean.data.stream.csv import CSVFile
from openclean.data.types import DatasetSchema
from openclean.profiling.datatype.convert import DatatypeConverter


def dataset(
    filename: str, header: Optional[DatasetSchema] = None,
    delim: Optional[str] = None, compressed: Optional[bool] = None,
    typecast: Optional[DatatypeConverter] = None, none_is: Optional[str] = None,
    encoding: Optional[str] = None
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
    header: list of string, default=None
        Optional header. If no header is given it is assumed that the first
        row in the CSV file contains the header information.
    delim: string, default=None
        The column delimiter used in the CSV file.
    compressed: bool, default=None
        Flag indicating if the file contents have been compressed using
        gzip.
    typecast: openclean.profiling.datatype.convert.DatatypeConverter,
            default=None
        Optional type cnverter that is applied to all data rows.
    none_is: string, default=None
        String that was used to encode None values in the input file. If
        given, all cell values that match the given string are substituted
        by None.
    encoding: string, default=None
        The csv file encoding e.g. utf-8, utf16 etc

    Returns
    -------
    pd.DataFrame
    """
    file = CSVFile(
        filename=filename,
        header=header,
        delim=delim,
        compressed=compressed,
        none_is=none_is,
        encoding=encoding
    )
    with file.open() as reader:
        data, index = list(), list()
        for _, rowid, row in reader:
            if typecast is not None:
                row = [typecast.cast(v) for v in row]
            data.append(row)
            index.append(rowid)
        return pd.DataFrame(data=data, columns=file.columns, index=index, dtype=object)
