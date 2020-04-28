# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data stream profiler that generates a set of distinct values in a stream."""

from openclean.data.column import as_list
from openclean.data.metadata import Feature
from openclean.data.stream import Stream
from openclean.profiling.base import DatastreamProfiler


def distinct(df, columns=None):
    """Compute the set of distinct value combinations for a given list of
    columns. Returns a single feature metadata object containing the distinct
    values (tuples) together with their frequency counts.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int or string or list(int or string), optional
        List of column index or column name for columns for which distinct
        value combinations are computed.

    Returns
    -------
    dict
    """
    # If the list of columns is not given, the set of distinct value
    # combinations over all columns in the data frame schema is computed.
    if columns is None:
        columns = as_list(df.columns)
    return Distinct().apply(Stream(df=df, columns=columns))


class Distinct(DatastreamProfiler):
    """Compute the set of distinct values from a data stream together with the
    value frequency counts.
    """
    def apply(self, stream):
        """Compute distinct values and their frequency counts for elements in
        the given stream of values.

        Parameters
        ----------
        stream: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.data.metadata.Feature
        """
        # The metadata array is a sub-class of collection.Counter. It will
        # automatically create counts for the values in the iterator.
        return Feature(stream)
