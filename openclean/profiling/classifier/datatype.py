# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes data type statistics and raw type domain
assignments for columns in a data frame.
"""

from collections import Counter

from openclean.data.sequence import Sequence
from openclean.function.list.distinct import Distinct
from openclean.function.list.helper import extract, merge, normalize
from openclean.function.value.classifier import ValueClassifier
from openclean.function.value.datatype import Datetime, Float, Int
from openclean.function.list.base import DictionaryFunction


"""Default data type classifier."""
DATATYPES = ValueClassifier(Datetime(), Int(), Float(), default_label='text')


"""Enumarate accepted values for the datatype features argument."""
BOTH = 'both'
DISTINCT = 'distinct'
TOTAL = 'total'
FEATURES = [BOTH, DISTINCT, TOTAL]


def datatypes(
    df, columns=None, classifier=None, normalizer=None, features=DISTINCT,
    labels=None
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

    Returns
    -------
    list
    """
    op = Datatypes(
        classifier=classifier,
        normalizer=normalizer,
        features=features,
        labels=labels
    )
    return op.map(Sequence(df=df, columns=columns))


class Datatypes(DictionaryFunction):
    """Compute data type frequency counts for values in a given list."""
    def __init__(
        self, classifier=None, normalizer=None, features=DISTINCT,
        labels=None
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

        Raises
        ------
        ValueError
        """
        # Ensure that a valid features value is given.
        if features not in FEATURES:
            raise ValueError('invalid features {}'.format(features))
        # Ensure that two labels are given if features is 'both'
        self.labels = labels if labels is not None else (DISTINCT, TOTAL)
        if features == BOTH and len(self.labels) != 2:
            raise ValueError('invalid labels list {}'.format(self.labels))
        # Use default classifier if no classifier is given.
        self.classifier = classifier if classifier is not None else DATATYPES
        self.normalizer = normalizer
        self.features = features

    def map(self, values):
        """Compute list of raw data types and their counts for each distinct
        value (pair) in the specified column(s). Type labels are assigned by
        the associated classifier.

        The result is a dictionary that maps the detected type labels to
        frequency counts. The structure of values in the dictionary is
        determined by the features property.

        If a normalizer is associated with thif object, the values in the
        returned dictionary are normalized.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values that are typed and
            summarized.

        Returns
        -------
        dict
        """
        # Compute distinct values and their frequency counts. Then call the
        # map_distinct method to compute the result.
        return self.map_distinct(Distinct().map(values))

    def map_distinct(self, values, label=None):
        """Compute list of raw data types and their counts for each distinct
        value (pair) in a dictionary of distinct values with their pre-computed
        absolute counts.

        If the label argument is given, the dictionary is assumed to be nested.
        The label identifies the element in the nested dictionaries that
        contains the absolute value frequencies.

        Parameters
        ----------
        values: dict
            Dictionary of distinct values and their frequency counts.
        label: string, default=None
            Element in a nested dictionary that contains the absolute value
            counts.

        Returns
        -------
        dict
        """
        if label is not None:
            values = extract(values, label)
        if self.features == BOTH:
            counts = dict()
            for value, count in values.items():
                type_label = self.classifier.eval(value)
                if type_label in counts:
                    c = counts[type_label]
                    c[DISTINCT] += 1
                    c[TOTAL] += count
                else:
                    counts[type_label] = {DISTINCT: 1, TOTAL: count}
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = merge(
                    normalize(
                        extract(counts, DISTINCT),
                        normalizer=self.normalizer
                    ),
                    normalize(
                        extract(counts, TOTAL),
                        normalizer=self.normalizer
                    ),
                    labels=self.labels
                )
        else:
            if self.features == DISTINCT:
                counts = Counter([self.classifier.eval(v) for v in values])
            elif self.features == TOTAL:
                counts = Counter()
                for value, count in values.items():
                    counts[self.classifier.eval(value)] += count
            else:
                raise ValueError('invalid features {}'.format(self.features))
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = normalize(counts, normalizer=self.normalizer)
        return counts
