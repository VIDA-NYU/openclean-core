# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Many operators in openclean operate on a sequence of scalar values or tuples
of schalar values. Sequences are it represented by iterators in Python (e.g.,
list). This module contains a factory pattern for creating iterators over a
single column or a set of columns in a pandas data frame.
"""

from openclean.data.schema import as_list, select_clause


class Sequence(object):
    """Factory pattern for a lists of values from a single data frame column
    or tuples from a list of columns.

    The main reason for having a separate sequence class for pandas data frames
    is to have a wrapper that supports reference to columns by name or index
    and that supports iteration over tuples from multiple columns. The sequence
    class is also capable to handle data frames with duplicate column names.
    """
    def __new__(cls, df, columns):
        """Create an list of values from either a single data frame column
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

        Raise
        -----
        ValueError
        """
        # If the list of columns is not given, the set of values over all
        # columns in the data frame schema is used.
        if columns is None:
            columns = as_list(df.columns)
        # Ensure that columns is a list.
        columns = columns if isinstance(columns, list) else [columns]
        # Get list of index positions for referenced columns.
        _, colindex = select_clause(df.columns, columns)
        # Use a different generator depending on the number of columns that
        # are referenced.
        if len(colindex) == 1:
            return list(single_column_iterator(df, colindex[0]))
        elif len(colindex) > 1:
            return list(multi_column_iterator(df, colindex))
        else:
            raise ValueError('empty column list')


# -- Column sequence iterators ------------------------------------------------

def single_column_iterator(df, colidx):
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


def multi_column_iterator(df, colidx):
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
