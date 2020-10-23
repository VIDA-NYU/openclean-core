# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Majority picker functions that select a single value from a counter object.
"""

from abc import ABCMeta, abstractmethod
from collections import Counter
from typing import Callable, Optional

from openclean.data.types import Value


# -- Most frequent value picker interface -------------------------------------

class MajorityCounter(metaclass=ABCMeta):
    """Interface for majority pickers that select a single value from a set of
    values for which their (absolute) frequency counts are given.
    """
    @abstractmethod
    def pick(
        self, values: Counter, default: Optional[Value] = None,
        raise_error: Optional[bool] = False
    ) -> Value:
        """Picker function that returns the most frequent value from the given
        counter. Different implementations may impose additional constraints on
        whether a value is selected or not. If no value is selected by the
        picker, either the default is returned or a ValueError is raised (if
        the raise_error flag is True).

        Parameters
        ----------
        values: collections.Counter
            Frequency counter for values. The most frequent value from the
            counter is returned if it satisfies additional (implementation-
            specific constraints).
        default: scalar or tuple, default=None
            Default value that is returned if the most frequent value does not
            satisfy the imposed constraints and the raise_error flag is False.
        raise_error: bool, default=False
            Raise a ValueError if the most frequent value does not satisfy the
            imposed constraints.

        Returns
        -------
        scalar or tuple

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()


# -- Picker implementations ---------------------------------------------------

class MostFrequentValue(MajorityCounter):
    """Majority picker that select the most frequent value. This picker returns
    the default value (or raises an error) if the given list of values
    is empty or there are multiple-most frequent values.

    THe picker allows to define an additional threshold (min. frequency) that
    the most-frequent value has to satisfy.
    """
    def __init__(
        self, threshold: Optional[float] = None,
        normalizer: Optional[Callable] = None
    ):
        """Initialize the optional min. frequency thrshold and normalizer that
        will be applied to frequency values.

        Parameters
        ----------
        threshold: float, default=None
            Additional frequency threshold for the selected value. Ignored if
            None.
        normalizer: callable, default=None
            Normalizer that is applied to the frequency value before selection.
        """
        self.threshold = threshold
        self.normalizer = normalizer

    def pick(
        self, values: Counter, default: Optional[Value] = None,
        raise_error: Optional[bool] = False
    ) -> Value:
        """Return the the most frequent value in the counter. If the counter is
        empty or contains multiple most-frequent values the default value is
        returned or a ValueError is raised.

        Parameters
        ----------
        values: collections.Counter
            Frequency counter for values.
        default: scalar or tuple, default=None
            Default value that is returned if the counter contains no values or
            multiple most-frequent values.
        raise_error: bool, default=False
            Raise a ValueError if the counter contains no values or multiple
            most-frequent values.

        Returns
        -------
        scalar or tuple

        Raises
        ------
        ValueError
        """
        # There are several cases where no value will be selected. One of them
        # is the case where there are no values in the list.
        if len(values) == 0:
            if not raise_error:
                return default
            raise ValueError('cannot pick from empty set')
        # If a threshold constraint was specified we first test if the most
        # frequent value satisfies the constraint.
        value, freq = values.most_common(1)[0]
        if self.threshold is not None:
            if self.normalizer is not None:
                f = sorted(self.normalizer(values.values()), reverse=True)[0]
            else:
                f = freq
            if f < self.threshold:
                if not raise_error:
                    return default
                raise ValueError('threshold not satisfied by {}'.format(f))
        if len(values) > 1:
            # Ensure that the second most-frequent value is smaller that the
            # first.
            if freq == values.most_common(2)[1][1]:
                if not raise_error:
                    return default
                raise ValueError('multiple most-frequent values')
        return value


class OnlyOneValue(MajorityCounter):
    """Majority picker that only selects a value if the given counter contains
    exactly one value.
    """
    def pick(
        self, values: Counter, default: Optional[Value] = None,
        raise_error: Optional[bool] = False
    ) -> Value:
        """Return the only value in the given counter. If the counter contains
        no value or more than one value the default value is returned or a
        ValueError is raised.

        Parameters
        ----------
        values: collections.Counter
            Frequency counter for values. The most frequent value from the
            counter is returned if the counter contains exectly one value.
        default: scalar or tuple, default=None
            Default value that is returned if the counter contains no values or
            multiple values.
        raise_error: bool, default=False
            Raise a ValueError if the counter contains no values or multiple
            values.

        Returns
        -------
        scalar or tuple

        Raises
        ------
        ValueError
        """
        if len(values) == 1:
            return values.most_common(1)[0][0]
        elif not raise_error:
            return default
        raise ValueError('received set of {} values'.format(len(values)))
