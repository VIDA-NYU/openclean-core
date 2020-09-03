# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to perform groupby operations on a pandas dataframe."""

from openclean.data.groupby import DataFrameGrouping
from openclean.operator.base import DataFrameMapper
from openclean.util import ensure_callable

class GroupBy(DataFrameMapper):
    """GroupBy class that takes in the 'to group on' column names and a new index generator function (optional),
    performs the groupby and returns a DataFrameGrouping object"""
    def __init__(self, on, func=None):
        """Initialize the column names and an optional function.

        Parameters
        ----------
        on: list or string
            The column names to group by on
        func: any
            Object is tested for being a callable.
        """
        super(GroupBy, self).__init__()
        self.on = on
        self.func = ensure_callable(func) if func is not None else None

    def _transform(self, df):
        """Applies the groupby function and returns a pandas.groupby object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        _______
        pandas.groupby
        """
        if self.func != None:
            # generate a new index using the provided function and groupby this index
            lambda_func = df[self.on].apply(tuple, axis=1).apply(self.func)
            return df.groupby(lambda_func)
        # if no func provided, groupby on the provided columns
        return df.groupby(self.on)

    def map(self, df):
        """transforms and maps a pandas DataFrame into a DataFrameGrouping object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        _______
        DataFrameGrouping
        """
        groupedby = self._transform(df=df).groups
        grouping = DataFrameGrouping(df=df)
        for gby in groupedby:
            grouping.add(key=gby, rows=groupedby[gby])
        return grouping
