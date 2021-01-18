# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operator that creates feature vectors (embeddings) for a list of values.
Embeddings are generated by a vector generator that is applied to each element
in a given stream of scalar values or tuples of scalar values.
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict

import numpy as np

from openclean.data.sequence import Sequence


# -- Embedding function -------------------------------------------------------

def embedding(df, columns, features):
    """Compute feature vectors (embeddings) for values in a given (list of)
    data frame column (s). Computes a feature vector for each value using the
    given vector generator.

    Returns an n-dimensional feature vector where n is the number of features.
    The array has one row per value in the selected data frame column(s).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame
    columns: int or string or list(int or string)
        List of column index or column name for columns for which distinct
        value combinations are computed.
    features: openclean.profiling.embedding.base.ValueEmbedder
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).

    Returns
    -------
    openclean.embedding.base.FeatureVector
    """
    op = Embedding(features=features)
    return op.exec(values=Sequence(df=df, columns=columns))


# -- Embedding classes --------------------------------------------------------

class FeatureVector(OrderedDict):
    """Maintain a list values (e.g., the distinct values in a data frame
    column) together with their feature vectors (e.g., word embeddings).
    """
    def add(self, value, vec):
        self[value] = vec

    @property
    def data(self):
        """Get numpy array containing all feature vectors. The order of vectors
        is the same as the order in whch they were added.

        Returns
        -------
        numpy.array
        """
        return np.array([row for row in self.values()])


class Embedding(object):
    """Compute feature vectors for values in a given stream of scalar values
    or tuples of scalar values.
    """
    def __init__(self, features):
        """Initialize the vector of feature functions that are applied to each
        of the values in the input data stream.

        Parameters
        ----------
        features: openclean.profiling.embedding.base.ValueEmbedder
            Generator for feature vectors that computes a vector of numeric
            values for a given scalar value (or tuple).
        """
        self.features = features

    def exec(self, values):
        """Return an array that contains a feature vector for each distinct
        value in the given input data list. The vector is computed using a list
        of value feature functions. The resulting array has one column per
        feature function and one entry per distinct value.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        openclean.embedding.base.FeatureVector
        """
        # Prepare the associated feature generator. Make sure to use the full
        # list of values when pre-computing statistics. This may return a
        # modified feature vector generator.
        features = self.features.prepare(values)
        vec = FeatureVector()
        # Use the distinct set of values to generate vector embeddings.
        for value in set(values):
            vec.add(value, features.embed(value))
        return vec


class ValueEmbedder(metaclass=ABCMeta):
    """Abstract generator class of value (word) embeddings for scalar values.
    Outputs a feature vector for each value.
    """
    @abstractmethod
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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def prepare(self, values):
        """Passes the list of values to the vector generator pre-compute any
        statistics (e.g., min-max values) that are required.

        Parameters
        ----------
        values: iterable
            List of data values.

        Returns
        -------
        openclean.embedding.base.ValueEmbedder
        """
        raise NotImplementedError()  # pragma: no cover
