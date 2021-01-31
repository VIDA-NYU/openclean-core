# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Majority picker functions that select a single value from a counter object.
"""

from abc import ABCMeta, abstractmethod
from collections import Counter
from typing import Callable, List, Optional, Union

from openclean.data.types import Value
from openclean.function.value.base import CallableWrapper, ConstantValue, ValueFunction
from openclean.util.core import scalar_pass_through


# -- Most frequent value picker interface -------------------------------------

class ValuePicker(ValueFunction, metaclass=ABCMeta):
    """Value function that is used to select a single value, e.g., the most
    frequent value, from a list of values. The picker acts as a value function
    that picks a value when the function is prepared and then returns a constant
    function that returns the picked value for any input value. This type of
    behavior is for example needed for majority voting where we want to replace
    all values in a given attribute with a single (most frequent) value.
    """
    def __init__(self, raise_error: Optional[bool] = False):
        """Initialize the behavior for the value function's prepare method if
        no value in the given input list satisfies the constraints that are
        defined by the value picker implementation. By default, the value picker
        returns a scalar pass-trough function if no value is selected during
        prepare, i.e., the pick method is called with the raise_error flag set
        to False and a scalar_pass_through as the default value. The reason is
        that if we want to use the returned function to update values in a
        dataset attribute and there is no most frequent value then we do not
        want to replace any values but keep the original ones. If the user wants
        an error to be raised instead they can override the default behavior by
        setting the raise_error flag when the class is instantiated.

        Parameters
        ----------
        raise_error: bool, default=False
            Raise a ValueError in the prepare method if the pick method does
            not select a value for the given inputs.
        """
        self.raise_error = raise_error

    def eval(self, value: Value) -> Value:
        """The value picker requires to be prepared. It returns a new value
        function  as the preparation result. The eval method of this class
        therefore raises an error if called.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Raises
        -------
        NotImplementedError
        """
        raise NotImplementedError()

    def is_prepared(self) -> bool:
        """The value picker needs to be prepared.

        Returns
        -------
        bool
        """
        return False

    @abstractmethod
    def pick(
        self, values: Counter, default: Optional[Union[Callable, Value]] = None,
        raise_error: Optional[bool] = False
    ) -> Union[Callable, Value]:
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
        default: scalar, tuple, or callable, default=None
            Default value that is returned if the most frequent value does not
            satisfy the imposed constraints and the raise_error flag is False.
        raise_error: bool, default=False
            Raise a ValueError if the most frequent value does not satisfy the
            imposed constraints.

        Returns
        -------
        scalar, tuple, or callabel

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()

    def prepare(self, values: List[Value]) -> ValueFunction:
        """Optional step to prepare the function for a given set of values.
        This step allows to compute additional statistics over the set of
        values.

        While it is likely that the given set of values represents the values
        for which the eval() function will be called, this property is not
        guaranteed.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        openclean.function.value.base.ValueFunction
        """
        # Pick the most frequent value that satisfies the addition constraints
        # of this picker. Use a scalar_pass_through as the default if no value
        # is selected and the user did not set the raise_error flag when the
        # object was constructed.
        value = self.pick(
            values=Counter(values),
            default=scalar_pass_through,
            raise_error=self.raise_error
        )
        if callable(value):
            # The default value has been returned.
            return CallableWrapper(func=value)
        else:
            # Return a constant value function for the picked value.
            return ConstantValue(value=value)


# -- Picker implementations ---------------------------------------------------

class MajorityVote(ValuePicker):
    """Majority picker that select the most frequent value. This picker returns
    the default value (or raises an error) if the given list of values
    is empty or there are multiple-most frequent values.

    THe picker allows to define an additional threshold (min. frequency) that
    the most-frequent value has to satisfy.
    """
    def __init__(
        self, threshold: Optional[float] = None,
        normalizer: Optional[Callable] = None,
        raise_error: Optional[bool] = False
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
        raise_error: bool, default=False
            Raise a ValueError in the picker method (instead of returning the
            default pass-through function) if a given counter is empty or has
            multiple most-frequent values.
        """
        super(MajorityVote, self).__init__(raise_error=raise_error)
        self.threshold = threshold
        self.normalizer = normalizer

    def pick(
        self, values: Counter, default: Optional[Union[Callable, Value]] = None,
        raise_error: Optional[bool] = False
    ) -> Value:
        """Return the the most frequent value in the counter. If the counter is
        empty or contains multiple most-frequent values the default value is
        returned or a ValueError is raised.

        Parameters
        ----------
        values: collections.Counter
            Frequency counter for values.
        default: scalar, tuple, or callable, default=None
            Default value that is returned if the counter contains no values or
            multiple most-frequent values.
        raise_error: bool, default=False
            Raise a ValueError if the counter contains no values or multiple
            most-frequent values.

        Returns
        -------
        scalar, tuple, or callabel

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


class OnlyOneValue(ValuePicker):
    """Majority picker that only selects a value if the given counter contains
    exactly one value.
    """
    def __init__(self, raise_error: Optional[bool] = False):
        """Initialize the raise_error flag in the super class.

        Parameters
        ----------
        raise_error: bool, default=False
            Raise a ValueError in the picker method if a given counter is empty
            or contains multiple values.
        """
        super(OnlyOneValue, self).__init__(raise_error=raise_error)

    def pick(
        self, values: Counter, default: Optional[Union[Callable, Value]] = None,
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
        default: scalar, tuple, or callable, default=None
            Default value that is returned if the counter contains no values or
            multiple values.
        raise_error: bool, default=False
            Raise a ValueError if the counter contains no values or multiple
            values.

        Returns
        -------
        scalar, tuple, or callabel

        Raises
        ------
        ValueError
        """
        if len(values) == 1:
            return values.most_common(1)[0][0]
        elif not raise_error:
            return default
        raise ValueError('received set of {} values'.format(len(values)))
