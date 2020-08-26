# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to identify FDViolations in a pandas dataframe."""

from openclean.data.groupby import DataFrameGrouping
from openclean.operator.base import DataFrameMapper

class FDViolation(DataFrameMapper):
    """FDViolation class that takes the left side and right side column names. identifies any tuples involved in
    FDViolations and returns them as a DataFrameGrouping object"""
    def __init__(self, lhs, rhs):
        """
        Initializes the FDViolation class with the left and right hand side column names.

        Parameters
        __________
        lhs: list or string
            column name(s) of the determinant set
        rhs: string
            column name of the dependent set
        """
        super(FDViolation, self).__init__()
        self.lhs = lhs
        self.rhs = rhs

    def _transform(self, df):
        """Identifies FDViolations in a pandas DataFrame and returns a pandas.groupby object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to find violations in

        Returns
        _______
        pandas.groupby
        """
        # Keep groups that have more than one distinct value for the attributes
        # of the right-hand-size of the FD.
        fgroups = df[df[self.lhs].duplicated(keep=False)]
        # Group dataframe by columns in the left-hand-side of the FD.
        return fgroups.groupby(self.lhs)

    def map(self, df):
        """Identifies FD violations and maps the pandas DataFrame into a DataFrameGrouping object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to find violations in

        Returns
        _______
        DataFrameGrouping
        """
        violations = self._transform(df=df).groups
        grouping = DataFrameGrouping(df=df)
        for violation in violations:
            grouping.add(key=violation, rows=violations[violation])
        return grouping
