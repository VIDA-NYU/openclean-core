# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
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
from typing import Dict, Optional

from openclean.data.types import Scalar
from openclean.function.value.null import is_empty
from openclean.profiling.base import DataStreamProfiler, DistinctSetProfiler
from openclean.profiling.datatype import DatatypeConverter, DefaultConverter
from openclean.profiling.stats import MinMaxCollector

import openclean.profiling.stats as stats


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
        converter: openclean.profiling.datatype.DatatypeConverter, default=None
            Datatype converter that is used to determing the type of the
            values in the data stream.
        """
        self.top_k = top_k
        self.converter = converter if converter else DefaultConverter()

    def process(self, values: Counter) -> Dict:
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
        # Filter empty values for computation of the remaining column
        # statistics.
        empty_count = 0
        non_empty_values = Counter()
        datatypes = dict()
        minmax = dict()
        for val, count in values.items():
            if is_empty(val):
                empty_count += count
            else:
                non_empty_values[val] = count
                conv_val, type_label = self.converter.convert(val)
                # Adjust (min,max) counts and datatype counts. If this is the
                # first time we encounter a type of type_label neither
                # dictionary will have an entry for that type. Ootherwise, both
                # will have an entry for that type.
                if type_label in minmax:
                    minmax[type_label].consume(conv_val)
                    datatypes[type_label]['total'] += count
                    datatypes[type_label]['distinct'] += 1
                else:
                    minmax[type_label] = MinMaxCollector(first_value=conv_val)
                    datatypes[type_label] = {'total': count, 'distinct': 1}
        return {
            'minmaxValues': minmax,
            'totalValueCount': sum(values.values()),
            'emptyValueCount': empty_count,
            'distinctValueCount': len(non_empty_values),
            'entropy': stats.entropy(non_empty_values),
            'topValues': non_empty_values.most_common(self.top_k),
            'datatypes': datatypes
        }


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
        converter: openclean.profiling.datatype.DatatypeConverter, default=None
            Datatype converter that is used to determing the type of the
            values in the data stream.
        """
        # Create variables for different parts of the profiling result. These
        # variables will be initialize in the open method for the profiler.
        self.minmax = None
        self.empty_count = None
        self.total_count = None
        self.datatypes = None
        self.converter = converter if converter else DefaultConverter()

    def close(self) -> Dict:
        """Return the dictionary with collected statistics at the end of the
        data stream.

        Returns
        -------
        dict
        """
        return {
            'minmaxValues': self.minmax,
            'totalValueCount': self.total_count,
            'emptyValueCount': self.empty_count,
            'datatypes': self.datatypes
        }

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
        # Adjust value counts. For all following operations we ignore values
        # that are empty.
        self.total_count += count
        if is_empty(value):
            self.empty_count += count
            return
        # Convert the given value and get the type label. Then adjust the
        # datatype counter and the (min,max) counts .
        val, type_label = self.converter.convert(value)
        self.datatypes[type_label] += count
        if type_label in self.minmax:
            self.minmax[type_label].consume(val)
        else:
            self.minmax[type_label] = MinMaxCollector(first_value=val)

    def open(self):
        """Initialize the internal variables that maintain different parts of
        the generated profiling result.
        """
        self.minmax = dict()
        self.empty_count = 0
        self.total_count = 0
        self.datatypes = Counter()
