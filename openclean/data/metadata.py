# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data structures to maintain metadata about sets of values."""

from abc import ABCMeta, abstractmethod
from collections import Counter, OrderedDict, defaultdict

import numpy as np


class Metadata(metaclass=ABCMeta):
    """Base interface for all metadata data structures. All structures provide
    base functionality that is similar to dictionaries. Provides access to the
    list of values (keys()) and individual items (get(key)). Implementing
    classes may also implement the __getitem__(key) and __len__ method.

    Data values (keys) are either scalar or tuples.
    """
    def __init__(self, *args, **kwargs):
        """Generic constructor."""
        pass

    @abstractmethod
    def get(self, key):
        """Get metadata about the given value. The return value is dependent on
        the implementation. It should either be a numeric value, a dictionary,
        or a numpy array.

        Returns
        -------
        scalar, dict or array
        """
        raise NotImplementedError()

    @abstractmethod
    def keys(self):
        """Get list of values about which the metadata is maintained.

        Returns
        -------
        list
        """
        raise NotImplementedError()


class Feature(Counter, OrderedDict, Metadata):
    """Extend a counter object (which itself is a sub-class of a dictionary) to
    maintain (numeric) feature values (e.g., counts) for a set of distinct
    values (e.g., the distinct values in a data frame column).

    Adds methods insert feature quantities based on a feature function and to
    compute a normalized version of the feature values.
    """
    def __init__(self, iterable=None, label='count'):
        """Initialize the super class with an optional iterable list or
        dictionary. Set the feature identifier.

        Parameters
        ----------
        iterable: list or dict, optional
            Values to initialize the dictionary with.
        label: string, optional
            Identifier for the maintained feature.
        """
        super(Feature, self).__init__(iterable)
        self.label = label

    def __call__(self, key):
        """Make the object callable to act like a lookup table.

        Parameters
        ----------
        key: string
            Dictionary key value.

        Returns
        -------
        int or float
        """
        return self[key]

    @property
    def data(self):
        """Convert the feature values into a numpy array.

        Returns
        -------
        numpy.array
        """
        return np.array(list(self.values()))

    def normalize(self, func):
        """Return a copy of the feature where the feature values haven been
        normalized using the given normalization function. If the function is
        a class object it is expected to be a class from the openclean
        normalization package.

        Parameters
        ----------
        func: callable or class
            Function that is used to normalize the feature values.

        Returns
        -------
        openclean.data.meatdata.Feature
        """
        func = func(self.values()) if isinstance(func, type) else func
        result = Feature()
        for key in self.keys():
            result[key] = func(self[key])
        return result

    def to_dict(self, label):
        """Convert the array to a metadata dictionary. Each key in the returned
        object is associated with the dictionary that has exactly one element.
        The element name is defined by the given label. The associated value is
        the value that is associated with the key for this array.

        Parameters
        ----------
        label: string
            Name of the element in the dictionaries of the returned object that
            is associated with the metadata property that is maintained in this
            array.

        Returns
        -------
        openclean.data.metadata.FeatureDict
        """
        metadict = FeatureDict()
        for key in self:
            metadict[key][label] = self[key]
        return metadict

    def value(self, key, label, default_value=None):
        """Implement value method from feature dictionary. Returns the default
        value if either the given key does not exist or if the label does not
        match the label that was specified when the object was initialized.

        Parameters
        ----------
        key: string
            Key value for feature array.
        label: string
            Identifier of the feature that is contained in the array.
        default_value: any, optional
            Default value for keys that do not have any value associated with
            the given label.
        raise_error: bool, optional
            Raise a KeyError if the dictionary associated with the key does not
            have a value associated with the given element label.

        Returns
        -------
        scalar

        Raises
        ------
        KeyError
        """
        # Return default value if the given label does not match the feature
        # label.
        if label != self.label:
            return default_value
        return self.get(key, default_value)


class FeatureDict(defaultdict, Metadata):
    """Data structure to manage multiple metadata values for each element in a
    set of scalar values or tuples.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the super class with any given arguments."""
        super(FeatureDict, self).__init__(Counter, *args, **kwargs)

    def to_scalar(self, label, default_value=None):
        """Create a metadata object for one of the features that are maintained
        in the dictionaries that are associated with key values. The default
        value is used for entries that to not contain any value for the given
        element label.

        Parameters
        ----------
        label: string
            Label of element for which the metadata array is created.
        default_value: any, optional
            Default value for keys that do not have any value associated with
            the given label.

        Returns
        -------
        openclean.data,metadata.Feature

        Raises
        ------
        KeyError
        """
        meta_array = Feature(label=label)
        for key, values in self.items():
            meta_array[key] = values.get(label, default_value)
        return meta_array

    def value(self, key, label, default_value=None):
        """Get value for a given feature in the maintained dictionaries. The
        default value will be returned if the key or label does not exist.

        Parameters
        ----------
        key: string
            Key value for feature array.
        label: string
            Identifier of the feature that is contained in the array.
        default_value: any, optional
            Default value for keys that do not have any value associated with
            the given label.

        Returns
        -------
        scalar

        Raises
        ------
        KeyError
        """
        return self.get(key, dict()).get(label, default_value)


class FeatureVector(OrderedDict, Metadata):
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
