# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic classifier that can be used as a profiling function."""

from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple, Union

from openclean.data.types import Scalar
from openclean.function.value.base import ValueFunction
from openclean.function.value.classifier import ValueClassifier
from openclean.profiling.base import DataStreamProfiler

import openclean.function.value.base as base  # extract, merge, normalize


class ResultFeatures(Enum):
    """Enumarate accepted values for the datatype features argument."""
    DISTINCT = 0
    TOTAL = 1
    BOTH = 2


class Classifier(DataStreamProfiler):
    """The classifier wraps a ValueClassifier with functionality that allows
    it to be used as a profiling function.
    """
    def __init__(
        self, classifier: ValueClassifier,
        normalizer: Optional[Union[Callable, ValueFunction]] = None,
        features: Optional[ResultFeatures] = None,
        labels: Optional[Union[List[str], Tuple[str, str]]] = None
    ):
        """Initialize the individual classifier and object properties.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier,
                default=None
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
        """
        # Ensure that a valid features value is given.
        if features is None:
            features = ResultFeatures.TOTAL
        elif features not in ResultFeatures:
            raise TypeError('invalid features {}'.format(features))
        # Create keyword argument dictionary
        self.classifier = classifier
        self.normalizer = normalizer
        self.features = features
        # Ensure that two labels are given if features is 'both'
        self.labels = labels if labels is not None else ('distinct', 'total')
        if features == ResultFeatures.BOTH and len(self.labels) != 2:
            raise ValueError('invalid labels list {}'.format(self.labels))
        # Dictionary with distinct and total counts for each class label.
        self.counts = None

    def close(self) -> Dict:
        """Convert the total and distinct counts for class labels into the
        requested format. The result is a dictionary. The elements in the
        dictionary depend on the features that were requested (at object
        construction) and whether a normaizer was given or not.

        Returns
        -------
        dict
        """
        if self.features == ResultFeatures.BOTH:
            counts = dict()
            lbl_distinct, lbl_total = self.labels
            for key, freq in self.counts.items():
                dc, tc = freq
                counts[key] = {lbl_distinct: dc, lbl_total: tc}
            if self.normalizer is not None:
                # Normalize the results if a normalizer is given.
                counts = base.merge(
                    base.normalize(
                        base.extract(counts, self.labels[0]),
                        normalizer=self.normalizer
                    ),
                    base.normalize(
                        base.extract(counts, self.labels[1]),
                        normalizer=self.normalizer
                    ),
                    labels=self.labels
                )
        else:
            # Depending on which feature was selected for output we use the
            # distinct value count or the total value count from the tuples
            # in the counts dictionary.
            if self.features == ResultFeatures.DISTINCT:
                idx = 0
            elif self.features == ResultFeatures.TOTAL:
                idx = 1
            else:
                raise RuntimeError('invalid features {}'.format(self.features))
            # Normalize the results if a normalizer is given.
            counts = dict()
            for type_label, c in self.counts.items():
                counts[type_label] = c[idx]
            if self.normalizer is not None:
                counts = base.normalize(counts, normalizer=self.normalizer)
        return counts

    def consume(self, value: Scalar, count: int):
        """Consume a pair of (value, count) in the data stream. Collects all
        values in a counter dictionary.

        Parameters
        ----------
        value: scalar
            Scalar column value from a dataset that is part of the data stream
            that is being profiled.
        count: int
            Frequency of the value. Note that this count only relates to the
            given value and does not necessarily represent the total number of
            occurrences of the value in the stream.
        """
        # Get class label for the given value.
        label = self.classifier.eval(value)
        # Update distinct and total counts for the label.
        dc, tc = self.counts.get(label, (0, 0))
        self.counts[label] = (dc + 1, tc + count)

    def open(self):
        """Initialize the counter for class label frequencies at the beginning
        of the stream.
        """
        self.counts = dict()
