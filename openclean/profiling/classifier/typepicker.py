# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type picker select one or more class labels based on statistics about the
frequency of occurrence for each label in a set of data values.
"""

from abc import ABCMeta, abstractmethod

from openclean.function.value.comp import get_threshold
from openclean.function.value.normalize import divide_by_total


# -- Generic interface --------------------------------------------------------

class TypePicker(metaclass=ABCMeta):
    """Generic interface for data type pickers. Selects one or more class
    labels based on counts of occurrence.
    """
    @abstractmethod
    def select(self, stats, key_distinct='distinct', key_total='total'):
        """Select one or more type labels based on the given statistics.
        Expects a feature dictionary that contains for each type label the
        number of distinct values with the label as well as the number of
        total values having the type label.

        Parameters
        ----------
        stats: openclean.data.metadata.FeatureDict
            Dictionary that contains distinct and total counts for each type
            label.
        key_distinct: string, optional
            Key to access distinct counts for a type label.
        key_total: string, optional
            Key to access total counts for a type label.

        Returns
        -------
        list
        """
        raise NotImplementedError()


# -- Basic implementations of the type picker interface -----------------------

MAJORITYPICKER_DISTINCT = 'distinct'
MAJORITYPICKER_TOTAL = 'total'

MAJORITYPICKER_STATS = [MAJORITYPICKER_DISTINCT, MAJORITYPICKER_TOTAL]


class MajorityTypePicker(TypePicker):
    """Default majority picker. Selects the most frequent type label. Allows to
    specify a threshold constraint that the most frequent label has to satisfy.
    The optional pick_one flag controls whether at most one label is returned
    or more.
    """
    def __init__(
        self, statistics='distinct', normalize=divide_by_total, threshold=None,
        pick_one=True
    ):
        """Define the label count statistics upon which the selection is based.
        Also allows to define a threshold that the most common value has to
        satisfy.

        Parameters
        ----------
        statistics: string, optional
            Determine whether to base the selection on the distinct label
            counts or the total label counts.
        normalize: callable or type
            Normalization function that is applied before the class labels are
            selected.
        threshold: callable, optional
            Threshold condition that the most common type has to satisfy.
        pick_one: bool, optional
            If True only the label with the highest score is returned (if it
            satisfies the constraeint). Otherwise, all labels that satisfy
            the constraint are returned.

        Raises
        ------
        ValueError
        """
        # Ensure that valid statistics are selected.
        if statistics not in MAJORITYPICKER_STATS:
            raise ValueError('invalid picker statistic %s' % (statistics))
        self.statistics = statistics
        self.normalize = normalize
        if threshold is not None:
            self.threshold = get_threshold(threshold)
        else:
            self.threshold = None
        self.pick_one = pick_one

    def select(self, stats, key_distinct='distinct', key_total='total'):
        """Select one or more type labels (from most frequent to least) that
        satisfy the threshold constraint.

        Parameters
        ----------
        stats: openclean.data.metadata.FeatureDict
            Dictionary that contains distinct and total counts for each type
            label.
        key_distinct: string, optional
            Key to access distinct counts for a type label.
        key_total: string, optional
            Key to access total counts for a type label.

        Returns
        -------
        list
        """
        # Get feature for the selected statistics.
        if self.statistics == MAJORITYPICKER_DISTINCT:
            key = key_distinct
        elif self.statistics == MAJORITYPICKER_TOTAL:
            key = key_total
        else:
            raise ValueError('invalid picker statistic %s' % (self.statistics))
        feature = stats.to_scalar(key)
        # Normalize vector if normalization function is given.
        if self.normalize is not None:
            feature = feature.normalize(self.normalize)
        # Return label(s) that satisfy the threshold.
        labels = list()
        for label, score in feature.most_common():
            if self.threshold is not None:
                if not self.threshold(score):
                    break
            labels.append(label)
            if self.pick_one:
                break
        return labels
