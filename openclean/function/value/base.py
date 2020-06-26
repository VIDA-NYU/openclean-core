# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base class for value function. Collection of basic helper functions."""

from abc import ABCMeta, abstractmethod

from collections import Counter

from openclean.data.sequence import Sequence

import openclean.util as util


# -- Profiling function ------------------------------------------------------

class ProfilingFunction(metaclass=ABCMeta):
    """Profiler for a set of distinct values. Profiling functions compute
    statistics or informative summaries over a set of (distinct) values.

    Each profiler implements the exec_distinct() method. The method consumes a
    dictionary of distinct values mapped to their respective frequency counts.
    The result type of each profiler is implementation dependent. It should
    either be a scalar value (e.g. for aggregators) or a dictionary.

    Each profiling function has a (unique) name. The name is used as the key
    value in a dictionary that composes the results of multiple profiling
    functions.
    """
    def __init__(self, name=None):
        """Initialize the function name.

        Parameters
        ----------
        name: string, default=None
            Unique function name.
        """
        self.name = name if name else util.funcname(self)

    def profile(self, values):
        """Compute one or more features over a list of values. Converts the
        given list into a dictionary of distinct values and returns the result
        of applying the exec_distinct() method on that dctionary.

        Parameters
        ----------
        values: list
            List of scalar values or tuples.

        Returns
        -------
        scalar or list or dict
        """
        return self.exec_distinct(Distinct().exec(values))

    @abstractmethod
    def profile_distinct(self, values):
        """Compute one or more features over a set of distinct values. This is
        the main profiling function that computes statistics or informative
        summaries over the given data values. It operates on a compact form of
        a value list that only contains the distinct values and their frequency
        counts.

        The return type of this function is implementation dependend.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        scalar or list or dict
        """
        raise NotImplementedError()


# -- Distinct -----------------------------------------------------------------

def distinct(
    df, columns=None, normalizer=None, keep_original=False, labels=None
):
    """Compute the set of distinct value combinations for a given list of
    columns. Returns a dictionary containing the distinct values (tuples)
    together with their frequency counts.

    If the optional normalization function is given, the frequency counts in
    the returned dictionary will be normalized. If the keep original flag is
    True, the returned dictionary will map key values to nested dictionaries
    that contain the original and the normalized value.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int or string or list(int or string), optional
        List of column index or column name for columns for which distinct
        value combinations are computed.
    normalizer: callable or openclean.function.value.base.ValueFunction,
            default=None
        Optional normalization function that will be used to normalize the
        frequency counts in the returned dictionary.
    keep_original: bool, default=False
        If the keep original value is set to True, the resulting dictionary
        will map key values to dictionaries. Each nested dictionary will
        have two elements, the original ('absolute') value and the
        normalized value.
    labels: list or tuple, default=('absolute', 'normalized')
        List or tuple with exactly two elements. The labels will only be
        used if the keep_original flag is True. The first element is the
        label for the original value in the returned nested dictionary and
        the second element is the label for the normalized value.

    Returns
    -------
    dict
    """
    op = Distinct(
        normalizer=normalizer,
        keep_original=keep_original,
        labels=labels
    )
    return op.exec(Sequence(df=df, columns=columns))


class Distinct(object):
    """Compute the set of distinct values in a given sequence together with
    their value frequency counts.
    """
    def __init__(
        self, normalizer=None, keep_original=False, labels=None, name=None
    ):
        """Initialize the optional normalizer. By default absolute frequency
        counts are returned for the distinct values.

        If the optional normalization function is given, the frequency counts
        in the returned dictionary will be normalized. If the keep original
        flag is True, the returned dictionary will map key values to nested
        dictionaries that contain the original and the normalized value.

        Parameters
        ----------
        normalizer: callable or openclean.function.value.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize the
            frequency counts in the returned dictionary.
        keep_original: bool, default=False
            If the keep original value is set to True, the resulting dictionary
            will map key values to dictionaries. Each nested dictionary will
            have two elements, the original ('absolute') value and the
            normalized value.
        labels: list or tuple, default=('absolute', 'normalized')
            List or tuple with exactly two elements. The labels will only be
            used if the keep_original flag is True. The first element is the
            label for the original value in the returned nested dictionary and
            the second element is the label for the normalized value.
        name: string, default=None
            Unique function name.
        """
        if name is None:
            name = 'distinctValueCount'
            if normalizer is not None:
                name += 'Normalized'
        self.name = name
        self.normalizer = normalizer
        self.keep_original = keep_original
        self.labels = labels
        self._name = name

    def exec(self, values):
        """Compute distinct values and their frequency counts for elements in
        the given sequence of scalar values or tuples of scalar values.

        Parameters
        ----------
        values: list (or iterable)
            List of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        # The collection.Counter class is a dictionary. It will automatically
        # create counts for the values in the iterator.
        counts = Counter(values)
        # Normalize the result if a normalizer is given.
        if self.normalizer is not None:
            counts = normalize(
                values=counts,
                normalizer=self.normalizer,
                keep_original=self.keep_original,
                labels=self.labels
            )
        return counts


# -- Abstract base class for value functions ----------------------------------

class ValueFunction(ProfilingFunction, metaclass=ABCMeta):
    """The abstract class for value functions defines the interface for methods
    that need to be implemented for preparing and evaluating the function.
    """
    def __init__(self, name=None):
        """Initialize the function name.

        Parameters
        ----------
        name: string, default=None
            Unique function name.
        to_dict: callable, default=None
            Function that accepts a value and a feature as arguments and that
            returns a dictionary.
        """
        super(ValueFunction, self).__init__(name=name)

    def apply(self, values):
        """Apply the function to each value in a given set. Returns a list of
        values that are the result of the eval method for the respective input
        values.

        Calls the prepare method before executing the eval method on each
        individual value in the given list.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        list
        """
        f = self.prepare(values)
        return [f.eval(v) for v in values]

    @abstractmethod
    def eval(self, value):
        """Evaluate the function on a given value. The value may either be a
        scalar or a tuple. The value will be from the list of values that was
        passed to the object in the prepare call.

        The return value of the function is implementation dependent.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        raise NotImplementedError()

    @abstractmethod
    def is_prepared(self):
        """Returns True if the prepare method is ignored by an implementation
        of this function. Containing classes will only call the prepare method
        for those value functions that are not prepared.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    def map(self, values):
        """The map function takes a list of values and outputs a dictionary.
        The keys in the returned dictionary are the distinct values in the
        input list. The values that are associated with the keys are the result
        of applying the eval function of this class on the key value.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        f = self.prepare(values)
        result = dict()
        for v in values:
            result[v] = f.eval(v)
        return result

    @abstractmethod
    def prepare(self, values):
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
        raise NotImplementedError()

    def profile_distinct(self, values):
        """By default the result of the map() function is used as the result
        if the value function is used as a data profiler.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        scalar or list or dict
        """
        return self.map(values)


class PreparedFunction(ValueFunction):
    """Abstract base class for value functions that do not make use of the
    prepare method. These functions are considered as initialized and ready
    to operate without the need for calling the prepare method first.
    """
    def __init__(self, name=None):
        """Initialize the function name.

        Parameters
        ----------
        name: string, default=None
            Unique function name.
        """
        super(PreparedFunction, self).__init__(name=name)

    def __call__(self, value):
        """Make the function callable for individual values.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        return self.eval(value)

    def is_prepared(self):
        """Instances of this class do not need to be further prepared.

        Returns
        -------
        bool
        """
        return True

    def prepare(self, values):
        """The prepare step is ignored for a wrapped callable.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.
        """
        return self


class CallableWrapper(PreparedFunction):
    """Wrapper for callable functions as value functions. This value function
    does not prepare the wrapped callable.
    """
    def __init__(self, func, name=None):
        """Initialize the wrapped callable function. Raises a ValueError if the
        function is not a callable.

        Parameters
        ----------
        func: callable
            Function that is wrapped as a value finction.
        name: string, default=None
            Unique function name. Uses the function name as default if not
            given.

        Raises
        ------
        ValueError
        """
        # Ensure that the given function is actually a callable.
        if not callable(func):
            raise ValueError('not a callable function')
        self.func = func
        super(CallableWrapper, self).__init__(
            name=name if name else util.funcname(self.func)
        )

    def eval(self, value):
        """Evaluate the wrapped function on a given value. The value may either
        be a scalar or a tuple. The return value of the function is dependent
        on the wrapped function.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        scalar or tuple
        """
        return self.func(value)


# -- Helper classes and functions ---------------------------------------------

def extract(values, label, raise_error=True, default_value=None):
    """Create a flat dictionary from a nested one. The resulting dictionary
    contains the same keys as the input dictionary. The associated values are
    the values from the nested dictionaries under the given label.

    If a nested value does not contain the given label as key a KeyError is
    raised if the raise error flag is True. If the flag is False the given
    default value is used instead.

    Parameters
    ----------
    values: dict
        Nested dictionary from which the values with the given label are
        extracted.
    label: string
        Label of element for which the metadata array is created.
    raise_error: bool, default=True
        Raise a KeyError if a nested dictionary value does not contain the
        given label as a key.
    default_value: any, default=None
        Default value for values that do not contain the the given label as a
        key.

    Returns
    -------
    openclean.data,metadata.Feature

    Raises
    ------
    KeyError
    """
    result = dict()
    for key, value in values.items():
        if raise_error and label not in value:
            raise KeyError('missing label for {}'.format(key))
        result[key] = value.get(label, default_value)
    return result


def merge(values_1, values_2, labels, join='inner'):
    """Merge two dictionaries. The resulting dictionary will map key values to
    dictionaries. Each nested dictionary has two elements, representing the
    values from the respective merged dictionary. The labels for these elements
    are defined by the labels argument.

    The join method allows for four types of merging:

    - inner: Keep only those keys that are in the intersection of both
        dictionaries.
    - outer: Keep all keys from the union of both dictionaries.
    - left-outer: Keep all keys from the first dictionary.
    - right-outer: Keep all keys from the second dictionary.

    Raises a ValueError if the number of given labels is not two or if an
    invalid join method is specified.

    Parameters
    ----------
    vaues_1: dict
        Left side of the join.
    values_2: dict
        Right side of the join.
    join: enum['inner', 'outer', 'left-outer', 'right-outer'], default='inner'
        Join method identifier.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    if len(labels) != 2:
        raise ValueError('invalid label list {}'.format(labels))
    label_1, label_2 = labels
    result = dict()
    if join == 'inner':
        for key, value in values_1.items():
            if key in values_2:
                result[key] = {label_1: value, label_2: values_2[key]}
    elif join == 'outer':
        for key, value in values_1.items():
            result[key] = {label_1: value, label_2: values_2.get(key)}
        # Add elements in the second dictionary that are not part of the
        # result yet.
        for key, value in values_2.items():
            if key not in result:
                result[key] = {label_1: None, label_2: value}
    elif join == 'left-outer':
        for key, value in values_1.items():
            result[key] = {label_1: value, label_2: values_2.get(key)}
    elif join == 'outer':
        for key, value in values_2.items():
            result[key] = {label_1: values_1.get(key), label_2: value}
    else:
        raise ValueError('invalid join method {}'.format(join))
    return result


def normalize(values, normalizer, keep_original=False, labels=None):
    """Normalize frequency counts in a given dictionary. Expects a dictionary
    where keys are mapped to numeric values. Applies the given normalization
    function on all values. Returns a dictionary where keys are mapped to the
    normalized values.

    If the keep_original flag is True, the original values are also included in
    the result. In this case, the keys in the resulting dictionary are mapped
    to dictionaries with two values. The default key values for the nested
    dictionary values are 'absolute' for the original value and 'normalized'
    for the normalized value. These names can be overridden by providing a list
    or tuple of labels with exactly two elements.

    Parameters
    ----------
    values: dict
        Dictionary that maps arbitrary key values to numeric values.
    normalizer: callable or openclean.function.value.base.ValueFunction,
            default=None
        Normalization function that will be used to normalize the numeric
        values in the given dictionary.
    keep_original: bool, default=False
        If the keep original value is set to True, the resulting dictionary
        will map key values to dictionaries. Each nested dictionary will have
        two elements, the original ('absolute') value and the normalized value.
    labels: list or tuple, default=('absolute', 'normalized')
        List or tuple with exactly two elements. The labels will only be used
        if the keep_original flag is True. The first element is the label for
        the original value in the returned nested dictionary and the second
        element is the label for the normalized value.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # Create an instance of the normalizer class if only the type is
    # given.
    if isinstance(normalizer, type):
        f = normalizer()
    else:
        f = normalizer
    if isinstance(f, ValueFunction):
        # Prepare the normalizer if it is a ValueFunction.
        f = f.prepare(values.values())
        counts = dict()
        # Normalize absolute counts.
        for key, value in values.items():
            counts[key] = f.eval(value)
    elif callable(f):
        # If this is a callable we assume that it is a value function.
        abs_counts = [v for k, v, in values.items()]
        normalized_counts = f(abs_counts)
        counts = dict()
        index = 0
        for key, _ in values.items():
            counts[key] = normalized_counts[index]
            index += 1
    else:
        raise ValueError('invalid normalizer type')
    # Merge the original dictionary with the normalized results if the
    # keep_original Flag is True.
    if keep_original:
        labels = labels if labels is not None else ('absolute', 'normalized')
        counts = merge(values, counts, labels=labels)
    return counts


def scalar_pass_through(value):
    """Pass-through method for single scalar values.

    Parameters
    ----------
    value: scalar
        Scalar cell value from a data frame row.

    Returns
    -------
    scalar
    """
    return value
