# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic class for normalized feature functions."""


class NormalizedFeature(object):
    """This class is a simple wraper around a feature function and a
    normalization function that is used to normalize the feature values.
    """
    def __init__(self, func, normalize):
        """Initialize the feature function and the normalization function.

        Parameters
        ----------
        func: callable
            Feature function.
        normalize: callable
            Normalization function.
        """
        self.func = func
        self.normalize = normalize

    def __call__(self, value):
        """Return normalized feature value for the given argument.

        Parameters
        ----------
        value: scalar
            Value in a data stream.

        Returns
        -------
        float
        """
        return self.normalize(self.func(value))
