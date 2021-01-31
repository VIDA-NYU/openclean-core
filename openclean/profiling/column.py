# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Default profiler for columns i a dataset. Defines a profiler for columns in
a in-memory dataset as well as for dataset streams.

The information that is collected by these profilers differs. The in-memory
profiler is able to collect additiona information (e.g., top-k values) that
the stream profiler cannot collect.
"""

from collections import Counter
from typing import Optional

from openclean.data.types import Scalar
from openclean.function.value.null import is_empty
from openclean.profiling.base import DataStreamProfiler, DistinctSetProfiler
from openclean.profiling.datatype.convert import (
    DatatypeConverter, DefaultConverter
)
from openclean.profiling.stats import MinMaxCollector

import openclean.profiling.stats as stats


# -- Profiling results --------------------------------------------------------

class ColumnProfile(dict):
    """Dictionary of profiling results for the openclean column profiler."""
    def __init__(
        self, converter: DatatypeConverter, values: Optional[Counter] = None,
        top_k: Optional[int] = None
    ):
        """
        """
        self.converter = converter
        # Initialize the internal statistic.
        self['totalValueCount'] = 0
        self['emptyValueCount'] = 0
        self['datatypes'] = Counter()
        self['minmaxValues'] = dict()
        # Consume the list of values if given.
        non_empty_values = Counter()
        if values is not None:
            for value, count in values.items():
                value = self.consume(value=value, count=count, distinct=True)
                if value is not None:
                    non_empty_values[value] += count
            self['distinctValueCount'] = len(non_empty_values)
            self['entropy'] = stats.entropy(non_empty_values)
            self['topValues'] = non_empty_values.most_common(top_k)

    def consume(
        self, value: Scalar, count: int, distinct: Optional[bool] = False
    ) -> Scalar:
        """Consume a pair of (value, count) in the data stream. Values in the
        stream are not guaranteed to be unique and may be passed to this
        consumer multiple times (with multiple counts).

        Returns the given value if it is not an empty value. Otherwise, the
        returned result in None.

        Parameters
        ----------
        value: scalar
            Scalar column value from a dataset that is part of the data stream
            that is being profiled.
        count: int
            Frequency of the value. Note that this count only relates to the
            given value and does not necessarily represent the total number of
            occurrences of the value in the stream.
        distinct: bool, default=False
            Count distinct and total values for data types if this flag is
            True.

        Returns
        -------
        scalar
        """
        # Adjust value counts. For all following operations we ignore values
        # that are empty.
        self['totalValueCount'] += count
        if is_empty(value):
            self['emptyValueCount'] += count
            return
        # Convert the given value and get the type label. Then adjust the
        # datatype counter and the (min,max) counts.
        datatypes = self['datatypes']
        minmax = self['minmaxValues']
        val, type_label = self.converter.convert(value)
        if type_label in minmax:
            minmax[type_label].consume(val)
            if distinct:
                datatypes[type_label]['total'] += count
                datatypes[type_label]['distinct'] += 1
            else:
                datatypes[type_label] += count
        else:
            minmax[type_label] = MinMaxCollector(first_value=val)
            if distinct:
                datatypes[type_label] = {'total': count, 'distinct': 1}
            else:
                datatypes[type_label] += count
        return value


# -- Column profiler ----------------------------------------------------------

class DefaultColumnProfiler(DistinctSetProfiler):
    """Default profiler for columns in a data frame. This profiler does
    maintain a set of distinct values and includes the most frequent values in
    the returned result dictionary. Also extends the basic column profiler with
    data types for all values in the column.

    The result schema for the returned dictionary is:

    {
        "minmaxValues": smallest and largest not-None value for each data type
            in the stream,
        "emptyValueCount": number of empty values in the column,
        "totalValueCount": number of total values (including empty ones),
        "distinctValueCount": number of distinct values in the column,
        "entropy": entropy for distinct values in the column,
        "topValues": List of most frequent values in the column,
        "datatypes": Counter of type labels for all non-empty values
    }
    """
    def __init__(
        self, top_k: Optional[int] = 10,
        converter: Optional[DatatypeConverter] = None
    ):
        """Initialize the number of top-k values that are returned and the
        optional converter that is used for data type detection.

        Parameters
        ----------
        top_k: int, default=10
            Include the k most frequent values in the result.
        converter: openclean.profiling.datatype.convert.DatatypeConverter,
                default=None
            Datatype converter that is used to determing the type of the
            values in the data stream.
        """
        self.top_k = top_k
        self.converter = converter if converter else DefaultConverter()

    def process(self, values: Counter) -> ColumnProfile:
        """Compute profile for given counter of values in teh column.

        Parameters
        ----------
        values: collections.Counter
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        return ColumnProfile(
            converter=self.converter,
            values=values,
            top_k=self.top_k
        )


class DefaultStreamProfiler(DataStreamProfiler):
    """Default profiler for columns in a data stream. This profiler does not
    maintain a set of distinct values due to the unkown size of the stream
    and the amount of memory that is required to maintain all values in the
    stream in an internal counter.

    Extends the basic column profiler with data types that are computed for
    each value in the stream as they arrive via the consumer method.

    The result schema for the returned dictionary is:

    {
        "minmaxValues": smallest and largest not-None value for each data type
            in the stream,
        "emptyValueCount": number of empty values in the stream,
        "totalValueCount": number of total values (including empty ones),
        "datatypes": Counter of type labels for all non-empty values
    }
    """
    def __init__(self, converter: Optional[DatatypeConverter] = None):
        """Create internal varianbles for collected statistics.

        Parameters
        ----------
        converter: openclean.profiling.datatype.convert.DatatypeConverter,
                default=None
            Datatype converter that is used to determing the type of the
            values in the data stream.
        """
        # Create variable for profiler results. This variable will be
        # initialized in the open method for the profiler.
        self.profiler = None
        self.converter = converter if converter else DefaultConverter()

    def close(self) -> ColumnProfile:
        """Return the dictionary with collected statistics at the end of the
        data stream.

        Returns
        -------
        dict
        """
        return self.profiler

    def consume(self, value: Scalar, count: int):
        """Consume a pair of (value, count) in the data stream. Values in the
        stream are not guaranteed to be unique and may be passed to this
        consumer multiple times (with multiple counts).

        Parameters
        ----------
        value: scalar
            Scalar column value from a dataset that is part of the data stream
            that is being profiled.
        count: int
            Frequency of the value. Note that this count only relates to the
            given value and does not necessarily represent the total number of
            occurrences of the value in the stream.
        """
        self.profiler.consume(value=value, count=count, distinct=False)

    def open(self):
        """Initialize the internal variables that maintain different parts of
        the generated profiling result.
        """
        self.profiler = ColumnProfile(converter=self.converter)
