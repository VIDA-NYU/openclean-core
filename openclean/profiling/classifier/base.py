# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic classifier that can be used as a profiling function."""

from collections import Counter

from openclean.function.base import Distinct, ProfilingFunction
from openclean.function.value.classifier import ValueClassifier

import openclean.function.base as base  # extract, merge, normalize


"""Enumarate accepted values for the datatype features argument."""
BOTH = 'both'
DISTINCT = 'distinct'
TOTAL = 'total'
FEATURES = [BOTH, DISTINCT, TOTAL]


class Classifier(ValueClassifier, ProfilingFunction):
    """The classifier cextends a ValueClassifier with fnctionality for the
    Profiling function.

    The ValueClassifier evaluates a list of predicates or conditions on a given
    value (scalar or tuple). Each predicate is associated with a class label.
    The corresponding class label for the first predicate that is satisfied by
    the value is returned as the classification result. If no predicate is
    satisfied by a given value the result is either a default label or a
    ValueError is raised if the raise error flag is set to True.
    """
    def __init__(
        self, classifier=None, name=None, normalizer=None, features=None,
        labels=None, none_label=None, default_label=None, raise_error=False
    ):
        """Initialize the individual classifier and object properties.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier,
                default=None
            Classifier that assigns data type class labels for scalar column
            values. Uses the standard classifier if not specified.
        name: string, default='classifier'
            Unique classifier name.
        normalizer: callable or openclean.function.base.ValueFunction,
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
        """
        # Ensure that a valid features value is given.
        if features is None:
            features = DISTINCT
        elif features not in FEATURES:
            raise ValueError('invalid features {}'.format(features))
        # Ensure that two labels are given if features is 'both'
        self.labels = labels if labels is not None else (DISTINCT, TOTAL)
        if features == BOTH and len(self.labels) != 2:
            raise ValueError('invalid labels list {}'.format(self.labels))
        # Ensure that the classifier is a list
        if not isinstance(classifier, list):
            classifier = [classifier]
        # Create keyword argument dictionary
        kwargs = {
            'none_label': none_label,
            'default_label': default_label,
            'raise_error': raise_error
        }
        super(Classifier, self).__init__(*classifier, **kwargs)
        self._name = name if name else classifier
        self.normalizer = normalizer
        self.features = features

    def exec(self, values):
        """The execute method of the profiling function is mapped to the map
        function of th ValueClassifier.

        Parameters
        ----------
        value: scalar
            Scalar data value that is being classified.

        Returns
        -------
        dict
        """
        return self.map(values)

    def map(self, values):
        """Compute a dictionary containing type labels as keys that are
        associated with frequency values. Frequencies are either counts of
        distinct values in a given list that are assigned a particular label
        or the count of total values that are assigned the label.

        If a normalizer is associated with this object, the values in the
        returned dictionary are normalized.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values that are
            classified and summarized.

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
            values = base.extract(values, label)
        if self.features == BOTH:
            counts = dict()
            for value, count in values.items():
                type_label = self.eval(value)
                if type_label in counts:
                    c = counts[type_label]
                    c[DISTINCT] += 1
                    c[TOTAL] += count
                else:
                    counts[type_label] = {DISTINCT: 1, TOTAL: count}
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = base.merge(
                    base.normalize(
                        base.extract(counts, DISTINCT),
                        normalizer=self.normalizer
                    ),
                    base.normalize(
                        base.extract(counts, TOTAL),
                        normalizer=self.normalizer
                    ),
                    labels=self.labels
                )
        else:
            if self.features == DISTINCT:
                counts = Counter([self.eval(v) for v in values])
            elif self.features == TOTAL:
                counts = Counter()
                for value, count in values.items():
                    counts[self.eval(value)] += count
            else:
                raise ValueError('invalid features {}'.format(self.features))
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = base.normalize(counts, normalizer=self.normalizer)
        return counts

    def name(self):
        """Unique name of the profiling function.

        Returns
        -------
        string
        """
        return self._name
