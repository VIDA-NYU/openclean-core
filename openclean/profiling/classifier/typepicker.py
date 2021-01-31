# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Type picker select one or more class labels based on statistics about the
frequency of occurrence for each label in a set of data values.

The classes in this module implement different strategies for assigning a
datatype to a list of values (e.g., a column in a data frame).
"""

from openclean.function.value.normalize import divide_by_total
from openclean.profiling.base import DistinctSetProfiler
from openclean.profiling.classifier.base import ResultFeatures
from openclean.profiling.classifier.datatype import Datatypes
from openclean.util.threshold import to_threshold


# -- Majority type picker ----------------------------------------------------

def majority_typepicker(
    df, columns=None, classifier=None, threshold=0, use_total_counts=False,
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
    df: pd.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    classifier: openclean.function.value.classifier.ValueClassifier
            , default=None
        Classifier that assigns data type class labels for scalar column
        values.
    threshold: callable or int or float, default=0
        Callable predicate or numeric value that is used to constrain the
        possible candidates based on their normalized frequency.
    use_total_counts: bool, default=False
        Use total value counst instead of distinct counts to compute type
        frequencies.
    at_most_one: bool, default=False
        Ensure that at most one data type is returned in the result. If the
        flag is True and multiple types have the maximum frequency, an empty
        dictionary will be returned.

    Returns
    -------
    dict
    """
    return MajorityTypePicker(
        classifier=classifier,
        threshold=threshold,
        use_total_counts=use_total_counts,
        at_most_one=at_most_one
    ).run(df=df, columns=columns)


class MajorityTypePicker(DistinctSetProfiler):
    """Pick the most frequent type assigned by a given classifier to the values
    in a given list. Generates a dictionary containing the most frequent
    type(s) as key(s) and their normalized frequency as the associated value.

    The majority of a type may be defined based on the distinct values in the
    given list or the absolute value counts. Allows to further restrict the
    choice by requiring the frequency of the selected type to be above a given
    threshold.
    """
    def __init__(
        self, classifier=None, threshold=0, use_total_counts=False,
        at_most_one=False
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
        threshold: callable or int or float, default=0
            Callable predicate or numeric value that is used to constrain the
            possible candidates based on their normalized frequency.
        use_total_counts: bool, default=False
            Use total value counst instead of distinct counts to compute type
            frequencies.
        at_most_one: bool, default=False
            Ensure that at most one data type is returned in the result. If the
            flag is True and multiple types have the maximum frequency, an
            empty dictionary will be returned.
        """
        self.classifier = classifier
        self.threshold = to_threshold(threshold)
        self.use_total_counts = use_total_counts
        self.at_most_one = at_most_one

    def process(self, values):
        """Select one or more type labels based on data type statistics that
        are computed over the given list of values using the associated
        classifier.

        Returns a dictionary where the key(s) are the selected type(s) and the
        values are normalized type frequencies (using divide_by_total). If no
        type satisfies the associated threshold or more than one type does but
        the ensure single type flag is True, the result is an empty dictionary.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        list
        """
        types = Datatypes(
            classifier=self.classifier,
            features=ResultFeatures.TOTAL if self.use_total_counts else ResultFeatures.DISTINCT,  # noqa: E501
            normalizer=divide_by_total
        ).process(values)
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


# -- Threshold type picker ----------------------------------------------------

def threshold_typepicker(
    df, columns=None, classifier=None, threshold=0, use_total_counts=False
):
    """Identify all types assigned by a given classifier to the values in a
    list having a frequency that exceeds a specified threshold. Generates a
    dictionary containing the types as keys and their normalized frequency as
    the associated value.

    The frequency of a type may be computed based on the distinct values in the
    given list or the absolute value counts.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    classifier: openclean.function.value.classifier.ValueClassifier
            , default=None
        Classifier that assigns data type class labels for scalar column
        values.
    threshold: callable or int or float, default=0
        Callable predicate or numeric value that is used to constrain the
        possible candidates based on their normalized frequency.
    use_total_counts: bool, default=False
        Use total value counst instead of distinct counts to compute type
        frequencies.
    """
    return ThresholdTypePicker(
        classifier=classifier,
        threshold=threshold,
        use_total_counts=use_total_counts
    ).run(df=df, columns=columns)


class ThresholdTypePicker(DistinctSetProfiler):
    """Identify all types assigned by a given classifier to the values in a
    list having a frequency that exceeds a specified threshold. Generates a
    dictionary containing the types as keys and their normalized frequency as
    the associated value.

    The frequency of a type may be computed based on the distinct values in the
    given list or the absolute value counts.
    """
    def __init__(
        self, classifier=None, threshold=0, use_total_counts=False
    ):
        """Initialize the classifier for data type assignement. The threshold
        constrains the results by requiring a type to have a minimal frequency.

        Parameters
        ----------
        classifier: openclean.function.value.classifier.ValueClassifier
                , default=None
            Classifier that assigns data type class labels for scalar column
            values.
        threshold: callable or int or float, default=0
            Callable predicate or numeric value that is used to constrain the
            possible candidates based on their normalized frequency.
        use_total_counts: bool, default=False
            Use total value counst instead of distinct counts to compute type
            frequencies.
        """
        self.classifier = classifier
        self.threshold = to_threshold(threshold)
        self.use_total_counts = use_total_counts

    def process(self, values):
        """Select one or more type labels based on data type statistics that
        are computed over the given list of values using the associated
        classifier.

        Returns a dictionary where the key(s) are the selected type(s) and the
        values are normalized type frequencies (using divide_by_total). If no
        type satisfies the associated threshold or more than one type does but
        the ensure single type flag is True, the result is an empty dictionary.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        list
        """
        types = Datatypes(
            classifier=self.classifier,
            features=ResultFeatures.TOTAL if self.use_total_counts else ResultFeatures.DISTINCT,  # noqa: E501
            normalizer=divide_by_total
        ).process(values)
        # Return an empty dictionary if the list of returned types is empty.
        if not types:
            return dict()
        # Find all types that satisfy the frequency constraint.
        result = dict()
        for label, freq in types.items():
            if self.threshold(freq):
                result[label] = freq
        return result
