# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to normalize values in a list (e.g., a data frame
column).
"""


class divide_by_total(object):
    """Divide values in a list by the sum over all values."""
    def __init__(self, values, raise_error=True):
        """Compute sum over values in the given list.

        Parameters
        ----------
        values: list
            List of numeric values.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        self.total = 0
        for v in values:
            if type(v) not in [int, float]:
                if raise_error:
                    raise ValueError('not a numeric value {}'.format(v))
            else:
                self.total += v
        self.total = float(self.total)

    def __call__(self, value):
        """Divide given value by the pre-computed sum over all values in the
        list. If the sum was zero the result will be zero.

        If the given value is not a numeric value either a ValueError is raised
        if the respective flag is True or the value is returned as is if the
        flag is False.

        Parameters
        ----------
        value: number
            Numeric value from the list that was used to initialize the object.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.

        Returns
        -------
        float
        """
        # At this point we assume that an error has been raised for non-numeric
        # values when the object was instantiated. Here, we return the value
        # 'as is' if it is not a numeric value.
        if type(value) not in [int, float]:
            return value
        return float(value) / self.total if self.total > 0 else 0


class max_abs_scale(object):
    """Divided values in a list by the absolute maximum over all values."""
    def __init__(self, values, raise_error=True):
        """Compute maximum over values in the given list.

        Parameters
        ----------
        values: list
            List of numeric values.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        """
        # Handle non-numeric values.
        numbers = list()
        for v in values:
            if type(v) not in [int, float]:
                if raise_error:
                    raise ValueError('not a numeric value {}'.format(v))
            else:
                numbers.append(v)
        self.maxval = float(max(numbers))

    def __call__(self, value):
        """Divide given value by the pre-computed maximum over all values in
        the list. If the maximum was zero the result will be zero.

        Parameters
        ----------
        value: number
            Numeric value from the list that was used to initialize the object.

        Returns
        -------
        float
        """
        # At this point we assume that an error has been raised for non-numeric
        # values when the object was instantiated. Here, we return the value
        # 'as is' if it is not a numeric value.
        if type(value) not in [int, float]:
            return value
        return float(value) / self.maxval if self.maxval > 0 else 0


class min_max_scale(object):
    """Normalize values in a list using min-max feature scaling."""
    def __init__(self, values, raise_error=True):
        """Compute minimum and maximum over values in the given list.

        Parameters
        ----------
        values: list
            List of numeric values.
        """
        # Handle non-numeric values.
        numbers = list()
        for v in values:
            if type(v) not in [int, float]:
                if raise_error:
                    raise ValueError('not a numeric value {}'.format(v))
            else:
                numbers.append(v)
        self.maxval = float(max(numbers))
        self.minval = float(min(numbers))

    def __call__(self, value):
        """Normalize value using min-max feature scaling. If the pre-computed
        minimum and maximum for the value list was zero the result will be
        zero.

        Parameters
        ----------
        value: number
            Numeric value from the list that was used to initialize the object.

        Returns
        -------
        float
        """
        # At this point we assume that an error has been raised for non-numeric
        # values when the object was instantiated. Here, we return the value
        # 'as is' if it is not a numeric value.
        if type(value) not in [int, float]:
            return value
        if self.minval != self.maxval:
            return (float(value) - self.minval) / (self.maxval - self.minval)
        else:
            return 0
