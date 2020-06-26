# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type picker select one or more class labels based on statistics about the
frequency of occurrence for each label in a set of data values.

The classes in this module implement different strategies for assigning a
datatype to a list of values (e.g., a column in a data frame).
"""

from openclean.data.sequence import Sequence
from openclean.function.value.base import DictionaryFunction
from openclean.function.value.normalize import divide_by_total
from openclean.profiling.classifier.base import DISTINCT, TOTAL
from openclean.profiling.classifier.datatype import Datatypes
from openclean.profiling.util import get_threshold


# -- Majority type picker ----------------------------------------------------

def majority_typepicker(
    df, columns=None, classifier=None, threshold=None, use_total_counts=False,
    at_most_one=False
):
    """Pick the most frequent type assigned by a given classifier to the values
    in a given list. Generates a dictionary containing the most frequent
    type(s) as key(s) and their normalized frequency as the associated value.

    The majority of a type may be defined based on the distinct values in the
    given list or the absolute value counts. Allows to further restrict the
    choice by requiring the frequency of the selected type to be above a given
    threshold.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) that are considered by the type
        picker.
    classifier: openclean.function.value.classifier.ValueClassifier
            , default=None
        Classifier that assigns data type class labels for scalar column
        values.
    threshold: callable or int or float
        Callable predicate or numeric value that is used to constrain the
        possible candidates based on their normalized frequency.
    use_total_counts: bool, default=False
        Use total value counst instead of distinct counts to compute type
        frequencies.
    at_most_one: bool, default=False
        Ensure that at most one data type is returned in the result. If the
        flag is True and multiple types have the maximum frequency, an
    """
    picker = MajorityTypePicker(
        classifier=classifier,
        threshold=threshold,
        use_total_counts=use_total_counts,
        at_most_one=at_most_one
    )
    return picker.map(Sequence(df=df, columns=columns))


class MajorityTypePicker(DictionaryFunction):
    """Pick the most frequent type assigned by a given classifier to the values
    in a given list. Generates a dictionary containing the most frequent
    type(s) as key(s) and their normalized frequency as the associated value.

    The majority of a type may be defined based on the distinct values in the
    given list or the absolute value counts. Allows to further restrict the
    choice by requiring the frequency of the selected type to be above a given
    threshold.
    """
    def __init__(
        self, classifier=None, threshold=None, use_total_counts=False,
        at_most_one=False, name=None
    ):
        """Initialize the classifier for data type assignement. The optional
        threshold allows to further constrain the possible results by requiring
        a minimal frequency for the picked type.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier
                , default=None
            Classifier that assigns data type class labels for scalar column
            values.
        threshold: callable or int or float
            Callable predicate or numeric value that is used to constrain the
            possible candidates based on their normalized frequency.
        use_total_counts: bool, default=False
            Use total value counst instead of distinct counts to compute type
            frequencies.
        at_most_one: bool, default=False
            Ensure that at most one data type is returned in the result. If the
            flag is True and multiple types have the maximum frequency, an
            empty dictionary will be returned.
        name: string, default='majorityTypePicker'
            Unique function name.
        """
        self.classifier = classifier
        self.threshold = get_threshold(threshold)
        self.use_total_counts = use_total_counts
        self.at_most_one = at_most_one
        self._name = name if name else 'majorityTypePicker'

    def map(self, values):
        """Select one or more type labels based on data type statistics that
        are computed over the given list of values using the associated
        classifier.

        Returns a dictionary where the key(s) are the selected type(s) and the
        values are normalized type frequencies (using divide_by_total). If no
        type satisfies the associated threshold or more than one type does but
        the ensure single type flag is True, the result is an empty dictionary.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values that are first
            typed to then select the most frequent type.

        Returns
        -------
        list
        """
        types = Datatypes(
            classifier=self.classifier,
            features=TOTAL if self.use_total_counts else DISTINCT,
            normalizer=divide_by_total
        ).map(values)
        # Return an empty dictionary if the list of returned types is empty.
        if not types:
            return dict()
        # Find the type with the frequency value. Return an empty dictionary if
        # the value does not satisfy the threshold.
        max_freq = max(types.values())
        if not self.threshold(max_freq):
            return dict()
        result = dict()
        for label, freq in types.items():
            if freq == max_freq:
                result[label] = freq
        # If more than one type matches the maximum frequency and the ensure
        # single result flag is True, return an empty dictionary.
        if self.at_most_one and len(result) > 1:
            return dict()
        return result

    def name(self):
        """Unique function name.

        Returns
        -------
        string
        """
        return self._name


# -- Threshold type picker ----------------------------------------------------

def threshold_typepicker(
    df, columns=None, classifier=None, threshold=None, use_total_counts=False
):
    """Identify all types assigned by a given classifier to the values in a
    list having a frequency that exceeds a specified threshold. Generates a
    dictionary containing the types as keys and their normalized frequency as
    the associated value.

    The frequency of a type may be computed based on the distinct values in the
    given list or the absolute value counts.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) that are considered by the type
        picker.
    classifier: openclean.function.value.classifier.ValueClassifier
            , default=None
        Classifier that assigns data type class labels for scalar column
        values.
    threshold: callable or int or float
        Callable predicate or numeric value that is used to constrain the
        possible candidates based on their normalized frequency.
    use_total_counts: bool, default=False
        Use total value counst instead of distinct counts to compute type
        frequencies.
    """
    picker = ThresholdTypePicker(
        classifier=classifier,
        threshold=threshold,
        use_total_counts=use_total_counts
    )
    return picker.map(Sequence(df=df, columns=columns))


class ThresholdTypePicker(DictionaryFunction):
    """Identify all types assigned by a given classifier to the values in a
    list having a frequency that exceeds a specified threshold. Generates a
    dictionary containing the types as keys and their normalized frequency as
    the associated value.

    The frequency of a type may be computed based on the distinct values in the
    given list or the absolute value counts.
    """
    def __init__(
        self, classifier=None, threshold=None, use_total_counts=False,
        name=None
    ):
        """Initialize the classifier for data type assignement. The threshold
        constrains the results by requiring a type to have a minimal frequency.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier
                , default=None
            Classifier that assigns data type class labels for scalar column
            values.
        threshold: callable or int or float
            Callable predicate or numeric value that is used to constrain the
            possible candidates based on their normalized frequency.
        use_total_counts: bool, default=False
            Use total value counst instead of distinct counts to compute type
            frequencies.
        name: string, default='thresholdTypePicker'
            Unique function name.
        """
        self.classifier = classifier
        self.threshold = get_threshold(threshold)
        self.use_total_counts = use_total_counts
        self._name = name if name else 'thresholdTypePicker'

    def map(self, values):
        """Select one or more type labels based on data type statistics that
        are computed over the given list of values using the associated
        classifier.

        Returns a dictionary where the key(s) are the selected type(s) and the
        values are normalized type frequencies (using divide_by_total). If no
        type satisfies the associated threshold or more than one type does but
        the ensure single type flag is True, the result is an empty dictionary.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values that are first
            typed to then select the most frequent type.

        Returns
        -------
        list
        """
        types = Datatypes(
            classifier=self.classifier,
            features=TOTAL if self.use_total_counts else DISTINCT,
            normalizer=divide_by_total
        ).map(values)
        # Return an empty dictionary if the list of returned types is empty.
        if not types:
            return dict()
        # Find all types that satisfy the frequency constraint.
        result = dict()
        for label, freq in types.items():
            if self.threshold(freq):
                result[label] = freq
        return result

    def name(self):
        """Unique function name.

        Returns
        -------
        string
        """
        return self._name
