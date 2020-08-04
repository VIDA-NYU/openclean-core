# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic classifier that can be used as a profiling function."""

from collections import Counter

from openclean.profiling.base import ProfilingFunction

import openclean.function.value.base as base  # extract, merge, normalize


"""Enumarate accepted values for the datatype features argument."""
BOTH = 'both'
DISTINCT = 'distinct'
TOTAL = 'total'
FEATURES = [BOTH, DISTINCT, TOTAL]


class Classifier(ProfilingFunction):
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
        labels=None
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
        """
        # Ensure that a valid features value is given.
        if features is None:
            features = DISTINCT
        elif features not in FEATURES:
            raise ValueError('invalid features {}'.format(features))
        # Create keyword argument dictionary
        super(Classifier, self).__init__(name=name)
        self.classifier = classifier
        self.normalizer = normalizer
        self.features = features
        # Ensure that two labels are given if features is 'both'
        self.labels = labels if labels is not None else (DISTINCT, TOTAL)
        if features == BOTH and len(self.labels) != 2:
            raise ValueError('invalid labels list {}'.format(self.labels))

    def run(self, values):
        """Compute list of raw data types and their counts for each distinct
        value (pair) in a dictionary of distinct values with their pre-computed
        absolute counts.

        Parameters
        ----------
        values: dict
            Dictionary of distinct values and their frequency counts.

        Returns
        -------
        dict
        """
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
                counts = Counter([self.classifier.eval(v) for v in values])
            elif self.features == TOTAL:
                counts = Counter()
                for value, count in values.items():
                    counts[self.classifier.eval(value)] += count
            else:
                raise ValueError('invalid features {}'.format(self.features))
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = base.normalize(counts, normalizer=self.normalizer)
        return counts
