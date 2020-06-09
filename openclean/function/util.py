# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper methods for lists and dictionaries of frequency
counts.
"""

from openclean.function.base import ValueFunction


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
    normalizer: callable or openclean.function.base.ValueFunction,
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
