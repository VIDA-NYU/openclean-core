# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes data type statistics and raw type domain
assignments for columns in a data frame.
"""

from openclean.data.sequence import Sequence
from openclean.function.value.datatype import Datetime, Float, Int
from openclean.profiling.classifier.base import Classifier


def datatypes(
    df, columns=None, classifier=None, normalizer=None, features=None,
    labels=None, none_label=None, default_label=None, raise_error=False
):
    """Compute list of raw data types and their counts for each distinct value
    (pair) in the specified column(s). Type labels are assigned by the given
    classifier.

    The result is a dictionary that maps the detected type labels to frequency
    counts. The resulting dictionary may contain a single value only or a pair
    of values. The format is determined by the features argument that accepts
    three different values:

    - distinct: Count the number of distinct values of a given type
    - total: Count the number of total values of a given type
    - both: Return a nested dictionary that contains both, the distinct count
        and the total count. The element names in the nested dictionary are
        given by the values in labels argument.

    If a normalizer is given, the values in the resulting dictionary are
    normalized.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) for which types are computed.
    classifier: openclean.function.value.classifier.ValueClassifier
            , default=None
        Classifier that assigns data type class labels for scalar column
        values. Uses the standard classifier if not specified.
    normalizer: callable or openclean.function.value.base.ValueFunction,
            default=None
        Optional normalization function that will be used to normalize the
        frequency counts in the returned dictionary.
    features: enum=['distinct', 'total', 'both'], default='distinct'
        Determines the values in the resulting dictionary.
    labels: list or tuple, default=('distinct', 'total')
        List or tuple with exactly two elements. The labels will only be
        used if the features argument is 'both'. The first element is the
        label for the distinct countsin the returned nested dictionary and
        the second element is the label for the total counts.
    none_label: string, default=None
        Label that is returned by the associated value functions to signal
        that a value did not satisfy the condition of the classifier.
    default_label: scalar, default=None
        Default label that is returned for values that do not satisfy the
        predicate.
    raise_error: bool, default=False
        Raise an error instead of returning the default label if no
        classifier was satisfied by a given value.

    Returns
    -------
    list
    """
    op = Datatypes(
        classifier=classifier,
        normalizer=normalizer,
        features=features,
        labels=labels,
        none_label=none_label,
        default_label=default_label,
        raise_error=raise_error
    )
    return op.map(Sequence(df=df, columns=columns))


class Datatypes(Classifier):
    """Compute data type frequency counts for values in a given list."""
    def __init__(
        self, classifier=None, name=None, normalizer=None, features=None,
        labels=None, none_label=None, default_label=None, raise_error=False
    ):
        """Initialize the associated classifier and optional normalizer.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier
                , default=None
            Classifier that assigns data type class labels for scalar column
            values. Uses the standard classifier if not specified.
        normalizer: callable or openclean.function.value.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize the
            frequency counts in the returned dictionary.
        features: enum=['distinct', 'total', 'both'], default='distinct'
            Determines the values in the resulting dictionary. Accepts three
            different values:
                - distinct: Count the number of distinct values of a given type
                - total: Count the number of total values of a given type
                - both: Return a nested dictionary that contains both, the
                    distinct count and the total count. The element names in
                    the nested dictionary are given by the values in labels
                    argument.
        labels: list or tuple, default=('distinct', 'total')
            List or tuple with exactly two elements. The labels will only be
            used if the features argument is 'both'. The first element is the
            label for the distinct counts in the returned nested dictionary and
            the second element is the label for the total counts.
        none_label: string, default=None
            Label that is returned by the associated value functions to signal
            that a value did not satisfy the condition of the classifier.
        default_label: scalar, default=None
            Default label that is returned for values that do not satisfy the
            predicate.
        raise_error: bool, default=False
            Raise an error instead of returning the default label if no
            classifier was satisfied by a given value.

        Raises
        ------
        ValueError
        """
        # Use a default set of class label functions if no classifier list is
        # given. The default label for unclassified values in the default
        # classifier is 'text'
        if classifier is None:
            classifier = [Datetime(), Int(), Float()]
            if default_label is None:
                default_label = 'text'
        super(Datatypes, self).__init__(
            classifier=classifier,
            name=name if name else 'datatypes',
            normalizer=normalizer,
            features=features,
            labels=labels,
            none_label=none_label,
            default_label=default_label,
            raise_error=raise_error
        )
