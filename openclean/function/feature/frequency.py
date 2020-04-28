# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that compute feature values for scalar cell values
base on the frequency of each value in a list (stream) of data values (e.g.,
in a data frame column).
"""

from openclean.data.metadata import Feature
from openclean.function.feature.base import FeatureFunctionFactory
from openclean.function.feature.normalize import NormalizedFeature
from openclean.function.value.normalize import min_max_scale


class NormalizedFrequency(FeatureFunctionFactory):
    """Factory that creates feature functions that compute the normalized
    frequency of each value in a data stream.
    """
    def get_function(self, data):
        """Compute the frequency for each value to be used as the feature
        function. Then initialize the normalization function using the list
        of value frequencies.

        Parameters
        ----------
        data: list
            List of value sin the stream.

        Returns
        -------
        callable
        """
        distinct = Feature(data)
        normalize = min_max_scale([distinct(v) for v in distinct.keys()])
        return NormalizedFeature(func=distinct, normalize=normalize)
