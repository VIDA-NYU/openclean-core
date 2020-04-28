# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that compute feature values for scalar cell values
base on the number of characters in the value's string representation.
"""

from openclean.function.feature.base import FeatureFunctionFactory
from openclean.function.feature.normalize import NormalizedFeature
from openclean.function.value.normalize import min_max_scale


class Length(object):
    """Compute length of the character string representation of a scalar value.
    """
    def __init__(self, as_string=True, default_value=None, raise_error=False):
        """Initialize parameters that control the length operator's behavior
        for values that are not of type string (that do not have a __len__
        method).

        Parameters
        ----------
        as_string: bool, optional
            Use string representation for all values if True.
        default_value: scalar, optional
            Default return value for values that do not have a __len__ method
            and if no error is raised.
        raise_error: bool, True
            Raise AttributeError for values that do not have a __len__ method.
        """
        self.as_string = as_string
        self.raise_error = raise_error
        self.default_value = default_value

    def __call__(self, value):
        """Return the length of the string representation of the given value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        int

        Raises
        ------
        ValueError
        """
        if self.as_string:
            return len(str(value))
        else:
            try:
                return len(value)
            except (AttributeError, TypeError) as ex:
                if self.raise_error:
                    raise ValueError(ex)
                return self.default_value


class NormalizedLength(FeatureFunctionFactory):
    """Factory that creates feature functions that compute the normalized
    length of a value over the length of all values in a data stream.
    """
    def get_function(self, data):
        """Create an instance of the value length function and instantiate the
        min-max feature scaling normalization function for the values in the
        given data stream.

        Parameters
        ----------
        data: list
            List of value sin the stream.

        Returns
        -------
        callable
        """
        feature_func = Length()
        normalize_func = min_max_scale([feature_func(v) for v in data])
        return NormalizedFeature(func=feature_func, normalize=normalize_func)
