# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to identify
violations of functional dependencies in a pandas dataframe.
"""

from openclean.data.groupby import DataFrameGrouping
from openclean.operator.map.groupby import GroupBy


def fd_violations(df, lhs, rhs=None):
    fdv = FDViolations(lhs=lhs, rhs=rhs)
    return fdv.map(df=df)


class FDViolations(GroupBy):
    """FDViolation class that takes the left side and right side column names.
    Identifies any tuples involved in FDViolations and returns them as a
    GroupBy object.
    """
    def __init__(self, lhs, rhs=None):
        """
        Initializes the FDViolation class with the left and right hand side column names.

        Parameters
        __________
        lhs: list or string
            column name(s) of the determinant set
        rhs: string
            column name of the dependent set
        """
        super(FDViolations, self).__init__(columns=lhs)
        self.rhs = rhs

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
        # Keep groups that have more than one distinct value on the left side for the attributes
        # of the right-hand-size of the FD.
        groups = self._transform(df=df)
        grouping = DataFrameGrouping(df=df)
        for key, rows in groups.items():
            if len(rows) > 1:
                grouping.add(key=key, rows=rows)
        return grouping
