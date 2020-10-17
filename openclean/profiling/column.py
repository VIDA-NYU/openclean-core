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
from openclean.profiling.classifier.base import ResultFeatures
from openclean.profiling.classifier.datatype import Datatypes


class DefaultColumnProfiler(DistinctSetProfiler):
    """Default profiler for columns in a data frame. This profiler does
    maintain a set of distinct values and includes the most frequent values in
    the returned result dictionary. Also extends the basic column profiler with
    data types for all values in the column.

    The result schema for the returned dictionary is:

    {
        "minimumValue": smallest not-None value in the column,
        'maximumValue": smallest not-None value in the column ,
        "emptyValueCount": number of empty values in the column,
        "totalValueCount": number of total values (including empty ones),
        "distinctValueCount": number of distinct values in the column,
        "topValues": List of most frequent values in the column,
        "datatypes": Counter of type labels for all non-empty values
    }
    """
    def __init__(
        self, top_k: Optional[int] = 10,
        label_datatypes: Optional[str] = 'datatypes',
        label_distinct_values: Optional[str] = 'distinctValueCount',
        label_empty_count: Optional[str] = 'emptyValueCount',
        label_min: Optional[str] = 'minimumValue',
        label_max: Optional[str] = 'maximumValue',
        label_top_values: Optional[str] = 'topValues',
        label_total_count: Optional[str] = 'totalValueCount'
    ):
        """Initialize the labels for elements in the profiling result.

        Parameters
        ----------
        top_k: int, default=10
            Include the k most frequent values in the result.
        label_datatypes: string, default='datatypes'
            Label for datatype counts in the result.
        label_distinct_values: string, default='distinctValueCount'
            Label for datatype counts in the result.
        label_empty_count: string, default='emptyValueCount'
            Label for empty value counts in the result.
        label_min: string, default='minimumValue'
            Label for minimum stream value in the result.
        label_max: string, default='maximumValue'
            Label for maximum stream value in the result.
        label_top_values: string, default='topValues'
            Label for k most frequent values included in the result.
        label_total_count: string, default='totalValueCount'
            Label for total value counts in the result.
        """
        self.top_k = top_k
        # Define the lables for elements in the result dictionary.
        self.label_datatypes = label_datatypes
        self.label_distinct_values = label_distinct_values
        self.label_empty_count = label_empty_count
        self.label_min = label_min
        self.label_max = label_max
        self.label_top_values = label_top_values
        self.label_total_count = label_total_count

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
        for val, count in values.items():
            if is_empty(val):
                empty_count += count
            else:
                non_empty_values[val] = count
        datatypes = Datatypes(features=ResultFeatures.BOTH)
        return {
            self.label_min: min(non_empty_values, default=None),
            self.label_max: max(non_empty_values, default=None),
            self.label_total_count: sum(values.values()),
            self.label_empty_count: empty_count,
            self.label_distinct_values: len(non_empty_values),
            self.label_top_values: non_empty_values.most_common(self.top_k),
            self.label_datatypes: datatypes.process(non_empty_values)
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
        "minimumValue": smallest not-None value in the stream,
        'maximumValue": smallest not-None value in the stream ,
        "emptyValueCount": number of empty values in the stream,
        "totalValueCount": number of total values (including empty ones),
        "datatypes": Counter of type labels for all non-empty values
    }
    """
    def __init__(
        self, label_datatypes: Optional[str] = 'datatypes',
        label_empty_count: Optional[str] = 'emptyValueCount',
        label_min: Optional[str] = 'minimumValue',
        label_max: Optional[str] = 'maximumValue',
        label_total_count: Optional[str] = 'totalValueCount'
    ):
        """Initialize the labels for elements in the profiling result.

        Parameters
        ----------
        label_datatypes: string, default='datatypes'
            Label for datatype counts in the result.
        label_empty_count: string, default='emptyValueCount'
            Label for empty value counts in the result.
        label_min: string, default='minimumValue'
            Label for minimum stream value in the result.
        label_max: string, default='maximumValue'
            Label for maximum stream value in the result.
        label_total_count: string, default='totalValueCount'
            Label for total value counts in the result.
        """
        # Create variables for different parts of the profiling result. These
        # variables will be initialize in the open method for the profiler.
        self.min = None
        self.max = None
        self.empty_count = None
        self.total_count = None
        self.datatypes = Datatypes()
        # Define the lables for elements in the result dictionary.
        self.label_empty_count = label_empty_count
        self.label_min = label_min
        self.label_max = label_max
        self.label_total_count = label_total_count
        self.label_datatypes = label_datatypes

    def close(self) -> Dict:
        """Return the dictionary with collected statistics at the end of the
        data stream.

        Returns
        -------
        dict
        """
        return {
            self.label_min: self.min,
            self.label_max: self.max,
            self.label_total_count: self.total_count,
            self.label_empty_count: self.empty_count,
            self.label_datatypes: self.datatypes.close()
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
        # that are or empty.
        self.total_count += count
        if is_empty(value):
            self.empty_count += count
            return
        # If either min or max is None this is the first not None value in the
        # stream.
        if self.min is None:
            self.min = value
            self.max = value
        elif value < self.min:
            self.min = value
        elif value > self.max:
            self.max = value
        # Add data type label for the value.
        self.datatypes.consume(value=value, count=count)

    def open(self):
        """Initialize the internal variables that maintain different parts of
        the generated profiling result.
        """
        self.min = None
        self.max = None
        self.empty_count = 0
        self.total_count = 0
        self.datatypes.open()
