# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value embedder for a list of feature functions."""

import numpy as np

from openclean.function.feature.base import FeatureFunctionFactory
from openclean.function.feature.character import (
    DigitsFraction, LettersFraction, SpecCharFraction, UniqueFraction,
    WhitespaceFraction
)
from openclean.function.feature.frequency import NormalizedFrequency
from openclean.function.feature.length import NormalizedLength
from openclean.profiling.embedding.base import ValueEmbedder


class FeatureEmbedding(ValueEmbedder):
    """Value embedder that uses a list of feature generating functions to
    create a vector for scalar input values.
    """
    def __init__(self, features):
        """Initialize the list of feature generating functions. Raises a
        ValueError if the feature list contains elements that are not callable
        or feature function factories.

        Parameters
        ----------
        features: list
            List of callables or feature generator factories. Factories are
            initialized when the prepare method is called.
        """
        # Ensure that features is a list that only contains callables or
        # feature function factories.
        self.features = features if isinstance(features, list) else [features]
        for f in self.features:
            if not callable(f) and not isinstance(f, FeatureFunctionFactory):
                raise ValueError('invalid feature funciton {}'.format(f))
        # Maintain size of feature vectore for faster access
        self.size = len(self.features)

    def embed(self, value):
        """Return the embedding vector for a given scalar value.

        Parameters
        ----------
        value: scalar
            Scalar value (or tuple) in a data stream.

        Returns
        -------
        numpy.array
        """
        data = np.zeros(self.size)
        for i in range(self.size):
            data[i] = self.features[i](value)
        return data

    def prepare(self, data):
        """Passes the list of values to the vector generator pre-compute any
        statistics (e.g., min-max values) that are required. Returns a
        (modified) instance of the feature generator.

        Parameters
        ----------
        data: list
            List of data values.

        Returns
        -------
        openclean.profiling.embedding.base.ValueEmbedder
        """
        # Check if the list of features contains any function factories. If
        # this is the case we initialize all of them with a copy of the data
        # stream.
        has_factories = False
        for f in self.features:
            if isinstance(f, FeatureFunctionFactory):
                has_factories = True
                break
        if not has_factories:
            # Return immediately if no functions need pre-computation
            return self
        # Get a copy of the data stream to pass it to the factories.
        feature_funcs = list()
        for f in self.features:
            if isinstance(f, FeatureFunctionFactory):
                f = f.get_function(data=data)
            feature_funcs.append(f)
        return FeatureEmbedding(features=feature_funcs)


class StandardFeatures(FeatureEmbedding):
    """Instance of the feature embedding function that uses a default set of
    seven value features to compute feature vectors. The computed features are:
    - normalized value length
    - normalized value frequency
    - uniqueness of characters in the value string
    - fraction of letter characters in the value string
    - fraction of digits in the value string
    - fraction of speical characters in the value string (not digit, letter, or
      whitespace)
    - fraction of whitespace characters in the value string
    """
    def __init__(self):
        """Initialize the list of default value feature functions."""
        super(StandardFeatures, self).__init__(
            features=[
                NormalizedLength(),
                NormalizedFrequency(),
                UniqueFraction(),
                LettersFraction(),
                DigitsFraction(),
                SpecCharFraction(),
                WhitespaceFraction()
            ]
        )
