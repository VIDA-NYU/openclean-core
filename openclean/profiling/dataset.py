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
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd


class DatasetProfile(list):
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
    def __init__(self):
        """Initialize the list that maintains the names of profiled columns,
        i.e., the dataset schema.
        """
        self.columns = []

    def add(self, name: str, stats: Dict):
        """Add profiler results for a given column to the list.

        Parameters
        ----------
        name: string
            Column name
        stats: dict
            Profiling results for the column.
        """
        self.append({'column': name, 'stats': stats})
        self.columns.append(name)

    def minmax(self, column: Union[int, str]) -> pd.DataFrame:
        """Get data frame with (min, max)-values for all data types in a given
        column.

        Raises a ValueError if the specified column is unknown.

        Parameters
        ----------
        column: int or string
            Column index of column name.

        Returns
        -------
        pd.DataFrame
        """
        if isinstance(column, int):
            stats = self[column]['stats']
        else:
            stats = None
            for col in self:
                if col['column'] == column:
                    stats = col['stats']
        if stats is None:
            raise ValueError("unknown column '{}'".format(column))
        types = stats['minmaxValues'].keys()
        data = list()
        for t in types:
            s = stats['minmaxValues'][t]
            row = [s['minimum'], s['maximum']]
            data.append(row)
        return pd.DataFrame(data, index=types, columns=['min', 'max'])

    def multitype_columns(self) -> DatasetProfile:
        """Get a dataset profiler that only contains information for those
        columns that have values of more than one raw data type.

        Returns
        -------
        openclean.profiling.dataset.DatasetProfiler
        """
        profile = DatasetProfile()
        for col, stats in self.profiles():
            if len(stats['datatypes']) > 1:
                profile.add(name=col, stats=stats)
        return profile

    def profiles(self) -> List[Tuple[str, Dict]]:
        """Get a list of (column name, profiling result) tuples for all columns
        in the dataset.

        Returns
        -------
        list
        """
        result = list()
        for i in range(len(self.columns)):
            result.append((self.columns[i], self[i]['stats']))
        return result

    def stats(self) -> pd.DataFrame:
        """Get a data frame containing the basic statistics for each columns.
        This includes the column name, the minimum and maximum value, the
        number of total values, empty values, and (if present) the number of
        distinct values per column.

        Returns
        -------
        pd.DataFrame
        """
        columns = ['total', 'empty', 'distinct', 'uniqueness', 'entropy']
        data = list()
        for obj in self:
            stats = obj['stats']
            total_count = stats['totalValueCount']
            empty_count = stats['emptyValueCount']
            distinct = stats.get('distinctValueCount')
            if distinct is not None:
                uniqueness = float(distinct) / float(total_count - empty_count)
            else:
                uniqueness = None
            row = [
                total_count,
                empty_count,
                distinct,
                stats.get('entropy'),
                uniqueness
            ]
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
        for obj in self:
            types.update(obj['stats']['datatypes'].keys())
        # Convert types to a sorted list of types.
        types = sorted(types)
        # Create a data frame with the type results. Datatype labels are used
        # as column names in the returned data frame.
        data = list()
        for obj in self:
            datatypes = obj['stats']['datatypes']
            row = list()
            for t in types:
                count = datatypes.get(t, 0)
                if isinstance(count, dict):
                    count = count['distinct'] if distinct else count['total']
                row.append(count)
            data.append(row)
        return pd.DataFrame(data=data, index=self.columns, columns=types)

    def unique_columns(self) -> DatasetProfile:
        """Get a dataset profiler that only contains information for those
        columns that have a uniqueness of 1, i.e., where all values are unique.

        Returns
        -------
        openclean.profiling.dataset.DatasetProfiler
        """
        profile = DatasetProfile()
        for col, stats in self.profiles():
            total_count = stats['totalValueCount']
            distinct_values = len(stats['distinctValueCount'])
            if total_count == distinct_values:
                profile.add(name=col, stats=stats)
        return profile
