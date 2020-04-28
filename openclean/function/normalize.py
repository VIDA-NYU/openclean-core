# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Apply factory functions for value normalization."""

from openclean.function.base import ApplyFactory

import openclean.function.value.normalize as vfunc


# -- Generic apply factory for normalization functions ------------------------

class NormalizeFactory(ApplyFactory):
    """Generic implementation of the factory pattern for normalization
    functions. Expects a class object that is instantiated with a list of
    values.
    """
    def __init__(self, normclass, raise_error=True):
        """Initialize the normalization class object which is instantiated
        every time the factory method is called.

        Parameters
        ----------
        normclass: class
            Class of a value normalization function.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        self.normclass = normclass
        self.raise_error = raise_error

    def get_func(self, df, colidx):
        """Get an instance of the normalization finctions for the values in
        the specified data frame column.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        colidx: int
            Index position of the column in the data frame.

        Returns
        -------
        callable
        """
        values = list(df.iloc[:, colidx])
        return self.normclass(values=values, raise_error=self.raise_error)


# -- Apply factories for common normalization functions. ----------------------

class DivideByTotal(NormalizeFactory):
    """Divide values in a list by the sum over all values."""
    def __init__(self, raise_error=True):
        """Initialize the object properties>

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        super(DivideByTotal, self).__init__(
            normclass=vfunc.divide_by_total,
            raise_error=raise_error
        )


class MaxAbsScale(NormalizeFactory):
    """Divided values in a list by the absolute maximum over all values."""
    def __init__(self, raise_error=True):
        """Initialize the object properties>

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        super(MaxAbsScale, self).__init__(
            normclass=vfunc.max_abs_scale,
            raise_error=raise_error
        )


class MinMaxScale(NormalizeFactory):
    """Normalize values in a list using min-max feature scaling."""
    def __init__(self, raise_error=True):
        """Initialize the object properties>

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        super(MinMaxScale, self).__init__(
            normclass=vfunc.min_max_scale,
            raise_error=raise_error
        )
