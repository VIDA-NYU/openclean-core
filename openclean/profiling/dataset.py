# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper class that provides added functionality of top of a list of column
profiling results.
"""

from __future__ import annotations
from typing import Dict, List, Optional

import pandas as pd


class DatasetProfiler(object):
    """THe dataset profiler provides functionality to access and transform the
    list of profiling results for columns in a dataset. Expects a list of
    dictionaries, each dictionary contaiing at least the following information
    about each column:

    - minimum value
    - maximum value
    - total number of values
    - total number of non-empty values
    - datatypes

    Additional information includes the distinct number of values with their
    respective frequency counts.
    """
    def __init__(
        self,
        profiles: List[Dict],
        label_datatypes: Optional[str] = 'datatypes',
        label_distinct_values: Optional[str] = 'distinctValueCount',
        label_empty_count: Optional[str] = 'emptyValueCount',
        label_min: Optional[str] = 'minimumValue',
        label_max: Optional[str] = 'maximumValue',
        label_total_count: Optional[str] = 'totalValueCount'
    ):
        """Initialize the profiling results and the labels for the respective
        profiing result components for each column.

        Parameters
        ----------
        profiles: list of dict
            Profiling results for each column in the dataset.
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
        label_total_count: string, default='totalValueCount'
            Label for total value counts in the result.
        """
        self.profiles = profiles
        self.label_datatypes = label_datatypes
        self.label_distinct_values = label_distinct_values
        self.label_empty_count = label_empty_count
        self.label_min = label_min
        self.label_max = label_max
        self.label_total_count = label_total_count
        # Extract list of columns names from the profiles.
        self.columns = [obj['column'] for obj in profiles]

    def multitype_columns(self) -> DatasetProfiler:
        """Get a dataset profiler that only contains information for those
        columns that have values of more than one raw data type.

        Returns
        -------
        openclean.profiling.dataset.DatasetProfiler
        """
        mtypecols = list()
        for obj in self.profiles:
            if len(obj['stats'][self.label_datatypes]) > 1:
                mtypecols.append(obj)
        return self._projection(mtypecols)

    def _projection(self, profiles: List[Dict]) -> DatasetProfiler:
        """Get an instance of the dataset profiler for a (limited) set of
        columns. This is a helper method since the assignment of element
        labels isn't changed by the projection.

        ----------
        profiles: list of dict
            Profiling results for columns in the returned dataset profiler.

        Returns
        -------
        openclean.profiling.dataset.DatasetProfiler
        """
        return DatasetProfiler(
            profiles=profiles,
            label_datatypes=self.label_datatypes,
            label_distinct_values=self.label_distinct_values,
            label_empty_count=self.label_empty_count,
            label_min=self.label_min,
            label_max=self.label_max,
            label_total_count=self.label_total_count
        )

    def stats(self) -> pd.DataFrame:
        """Get a data frame containing the basic statistics for each columns.
        This includes the column name, the minimum and maximum value, the
        number of total values, empty values, and (if present) the number of
        distinct values per column.

        Returns
        -------
        pd.DataFrame
        """
        columns = ['column', 'min', 'max', 'total', 'empty', 'distinct']
        data = list()
        for obj in self.profiles:
            stats = obj['stats']
            row = [
                obj['column'],
                stats[self.label_min],
                stats[self.label_max],
                stats[self.label_total_count],
                stats[self.label_empty_count]
            ]
            if self.label_distinct_values in stats:
                row.append(stats[self.label_distinct_values])
            else:
                row.append(None)
            data.append(row)
        return pd.DataFrame(data=data, index=self.columns, columns=columns)

    def types(self, distinct: Optional[bool] = False) -> pd.DataFrame:
        """Get a data frame containing type information for all columns that
        are included in the profiling results. For each column the number of
        total values for each each datatype that occurs in the dataset is
        included.

        If datatype information is divided into total and distinct counts the
        user has the option to get the cont of distinct values for each type
        instead of the total counts by setting the distinct flag to True.

        Parameters
        ----------
        distinct: bool, default=False
            Return type counts for distinct values instead of total counts.

        Returns
        -------
        pd.DataFrame
        """
        types = set()
        # Make a pass over the profiling results to get a list of all data
        # types that are present.
        for obj in self.profiles:
            types.update(obj['stats'][self.label_datatypes].keys())
        # Convert types to a sorted list of types.
        types = sorted(types)
        # Create a data frame with the type results. Datatype labels are used
        # as column names in the returned data frame.
        columns = ['column'] + types
        data = list()
        for obj in self.profiles:
            datatypes = obj['stats'][self.label_datatypes]
            row = [obj['column']]
            for t in types:
                count = datatypes.get(t, 0)
                if isinstance(count, dict):
                    count = count['distinct'] if distinct else count['total']
                row.append(count)
            data.append(row)
        return pd.DataFrame(data=data, index=self.columns, columns=columns)

    def unique_columns(self) -> DatasetProfiler:
        """Get a dataset profiler that only contains information for those
        columns that have a uniqueness of 1, i.e., where all values are unique.

        Returns
        -------
        openclean.profiling.dataset.DatasetProfiler
        """
        unqcols = list()
        for obj in self.profiles:
            stats = obj['stats']
            total_count = stats[self.label_total_count]
            distinct_values = len(stats[self.label_distinct_values])
            if total_count == distinct_values:
                unqcols.append(obj)
        return self._projection(unqcols)
