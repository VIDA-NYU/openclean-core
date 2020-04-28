# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for operators and functions that operate on a stream of
scalar values or tuples of scalar values.
"""

from openclean.data.column import select_clause


# -- Column stream factory ----------------------------------------------------

class Stream(object):
    """Factory pappern for iterators over values in a single data frame column
    or values from a list of multiple columns.
    """
    def __new__(cls, df, columns):
        """Create an interator for values in either a single data frame column
        or a list of data frame columns.

        Raises a ValueError if an empty column list is given or if the column
        list references columns that are unknown for the given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame.
        columns: int or string or list(int or string)
            Index or name of a data frame column or a list of column indexes
            or column names.
            value combinations are computed.

        Raise
        -----
        ValueError
        """
        columns = columns if isinstance(columns, list) else [columns]
        _, colindex = select_clause(df, columns)
        if len(colindex) == 1:
            return single_column_stream(df, colindex[0])
        elif len(colindex) > 1:
            return multi_column_stream(df, colindex)
        else:
            raise ValueError('empty column list')


# -- Column stream iterators --------------------------------------------------

def single_column_stream(df, colidx):
    """Iterator over values in a single data frame column.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.
    colidx:
        Index of the data frame column.

    Returns
    -------
    scalar
    """
    for _, values in df.iterrows():
        yield values.iloc[colidx]


def multi_column_stream(df, colidx):
    """Iterator over values in multiple columns in a data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.
    colidx:
        List of indexes for column in the data frame.

    Returns
    -------
    tuple
    """
    for _, values in df.iterrows():
        yield tuple([values.iloc[i] for i in colidx])
