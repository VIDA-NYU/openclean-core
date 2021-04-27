# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper class that provides added functionality of top of a list of column
profiling results.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from openclean.data.schema import select_clause
from openclean.data.stream.df import DataFrameStream
from openclean.data.types import Columns, ColumnRef, DatasetSchema
from openclean.operator.stream.consumer import StreamConsumer
from openclean.operator.stream.processor import StreamProcessor
from openclean.profiling.base import DataProfiler
from openclean.profiling.column import (
    DefaultColumnProfiler, DefaultStreamProfiler
)


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

    def column(self, name: ColumnRef) -> Dict:
        """Get the profiling results for a given column.

        Parameters
        ----------
        name: int or string
            Name or index position of the referenced column.

        Returns
        -------
        dict
        """
        _, colidx = select_clause(self.columns, name)
        return self[colidx[0]]['stats']

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
            if len(stats['datatypes']['total']) > 1:
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
            non_empty = total_count - empty_count
            if distinct is not None and non_empty > 0:
                uniqueness = float(distinct) / float(non_empty)
            else:
                uniqueness = None
            row = [
                total_count,
                empty_count,
                distinct,
                uniqueness,
                stats.get('entropy')
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
            types.update(obj['stats']['datatypes']['total'].keys())
        # Convert types to a sorted list of types.
        types = sorted(types)
        # Create a data frame with the type results. Datatype labels are used
        # as column names in the returned data frame.
        data = list()
        for obj in self:
            dt = obj['stats']['datatypes']
            row = list()
            for t in types:
                count = dt.get('distinct', dt.get('total', {})).get(t, 0)
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
            distinct_values = stats['distinctValueCount']
            if total_count == distinct_values:
                profile.add(name=col, stats=stats)
        return profile


# -- Dataset profiler ---------------------------------------------------------

class Profiler(metaclass=ABCMeta):  # pragma: no cover
    """Interface for data profiler that generate metadata for a given data
    frame.
    """
    @abstractmethod
    def profile(self, df: pd.DataFrame, columns: Optional[Columns] = None) -> Dict:
        """Run profiler on a given data frame. The structure of the resulting
        dictionary is implementatin dependent.

        TODO: define required components in the result of a data profier.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.
        columns: int, string, or list(int or string), default=None
            Single column or list of column index positions or column names for
            those columns that are being profiled. Profile the full dataset if
            None.

        Returns
        -------
        dict
        """
        raise NotImplementedError()


# -- Data stream profiling operators ------------------------------------------

"""Type alias for column profiler specifications."""
ColumnProfiler = Union[ColumnRef, Tuple[ColumnRef, DataProfiler]]
ProfilerSpecs = Union[ColumnProfiler, List[ColumnProfiler]]


class ProfileConsumer(StreamConsumer):
    def __init__(self, profilers: List[Tuple[int, str, DataProfiler]]):
        """Initialize the list of column profilers.

        Parameters
        ----------
        profilers: list of column index, name,  and
                openclean.profiling.base.ProfilingFunction
            List of profiling functions together with the index of the column
            that they are profiling.
        """
        self.profilers = profilers
        # Call the open method for each of the rofiling functions.
        for _, _, profiler in self.profilers:
            profiler.open()

    def close(self) -> List[Dict]:
        """Return a list containing the results from each of the profilers.

        Returns
        -------
        list
        """
        profile = DatasetProfile()
        for _, name, p in self.profilers:
            profile.add(name=name, stats=p.close())
        return profile

    def consume(self, rowid: int, row: List) -> List:
        """CDispatch extracted columns values to each consumer.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        for colidx, _, profiler in self.profilers:
            profiler.consume(value=row[colidx], count=1)


class ProfileOperator(StreamProcessor):
    def __init__(
        self, profilers: Optional[ColumnProfiler] = None,
        default_profiler: Optional[Type] = None
    ):
        """Profiling operator one or more columns in the data stream. By
        default all columns in the data stream are profiled independently using
        the default stream profiler. The optional list of profilers allows to
        override the default behavior by providing a list of column references
        and (optional) profiler functions.

        Parameters
        ----------
        profilers: list of tuples of column reference and
                openclean.profiling.base.ProfilingFunction, default=None
            Specify the list of columns that are profiled and the profiling
            function. If only a column reference is given (not a tuple) the
            default stream profiler is used for profiling the column.
        default_profiler: class, default=None
            Class object that is instanciated as the profiler for columns
            that do not have a profiler instance speicified for them.
        """
        self.profilers = profilers
        if default_profiler is None:
            default_profiler = DefaultStreamProfiler
        self.default_profiler = default_profiler

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream profiling consumers. Creates an instance
        of a stream profiler for each column that was selected for profiling.
        If no profilers were specified at object instantiation all columns will
        be profiled.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.profiling.dataset.ProfileConsumer
        """
        # Create a list of (column index, profiling function)-pairs that is
        # passed to the profiling cnsumer. That consumer will (i) open the
        # profilers, (ii) dispatch extracted row values to the consumers, and
        # (iii) generate the result from the profiling functions.
        consumers = list()
        if self.profilers is None:
            # If no profilers were specified at object creation all columns are
            # profiled.
            for i, name in enumerate(schema):
                consumers.append((i, name, self.default_profiler()))
        else:
            # Create profilers only for columns included in the profilers list
            # that was initialized at object creation.
            for p in self.profilers:
                # There are two types of values allowed in the profiler list.
                # Tuples that are (column reference, profiler function)-pairs
                # or column references only.
                if isinstance(p, tuple):
                    col, profiler = p
                else:
                    col = p
                    profiler = self.default_profiler()
                name, colidx = select_clause(schema, col)
                consumers.append((colidx[0], name[0], profiler))
        return ProfileConsumer(profilers=consumers)


def dataset_profile(
    df: pd.DataFrame, profilers: Optional[ColumnProfiler] = None,
    default_profiler: Optional[Type] = None
) -> DatasetProfile:
    """Profiling operator for profiling one or more columns in a data frame. By
    default all columns in the data stream are profiled independently using
    the default column profiler. The optional list of profilers allows to
    override the default behavior by providing a list of column references
    and (optional) profiler functions.

    Parameters
    ----------
    profilers: list of tuples of column reference and
            openclean.profiling.base.ProfilingFunction, default=None
        Specify the list of columns that are profiled and the profiling
        function. If only a column reference is given (not a tuple) the
        default profiler is used for profiling the column.
    default_profiler: class, default=None
        Class object that is instanciated as the profiler for columns
        that do not have a profiler instance speicified for them.
    """
    # Create a dataset stream for the given data frame.
    ds = DataFrameStream(df)
    if default_profiler is None:
        default_profiler = DefaultColumnProfiler
    return ProfileOperator(
        profilers=profilers,
        default_profiler=default_profiler
    ).open(schema=ds.columns).process(ds)
