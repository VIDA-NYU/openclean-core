# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value embedder for a list of feature functions."""

import numpy as np

from openclean.embedding.base import ValueEmbedder
from openclean.function.value.base import CallableWrapper, ValueFunction


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
            List of callables or openclean.function.value.base.ValueFunction.
            Value fnctions are initialized by the prepare method.
        """
        # Ensure that features is a list that only contains callables or value
        # functions
        self.features = list()
        if not isinstance(features, list):
            features = [features]
        for f in features:
            if not isinstance(f, ValueFunction):
                f = CallableWrapper(func=f)
            self.features.append(f)
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
            data[i] = self.features[i].eval(value)
        return data

    def prepare(self, values):
        """Passes the list of values to the vector generator pre-compute any
        statistics (e.g., min-max values) that are required. Returns a
        (modified) instance of the feature generator.

        Parameters
        ----------
        values: list
            List of data values.
        """
        prepared_features = list()
        for f in self.features:
            prepared_features.append(f.prepare(values))
        return FeatureEmbedding(prepared_features)
