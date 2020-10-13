# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes data type statistics and raw type domain
assignments for columns in a data frame.
"""

from openclean.function.value.classifier import ValueClassifier
from openclean.function.value.datatype import Datetime, Float, Int
from openclean.profiling.base import profile
from openclean.profiling.classifier.base import Classifier


def datatypes(
    df, columns=None, classifier=None, normalizer=None, features=None,
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
    df: pd.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
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
    dict
    """
    op = Datatypes(
        classifier=classifier,
        normalizer=normalizer,
        features=features,
        labels=labels
    )
    return profile(df, columns=columns, profilers=op)[op.name]


class Datatypes(Classifier):
    """Compute data type frequency counts for values in a given list."""
    def __init__(
        self, classifier=None, name=None, normalizer=None, features=None,
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
        # Use a default set of class label functions if no classifier list is
        # given. The default label for unclassified values in the default
        # classifier is 'text'
        if classifier is None:
            classifier = ValueClassifier(
                Datetime(),
                Int(),
                Float(),
                default_label='text'
            )
        super(Datatypes, self).__init__(
            classifier=classifier,
            name=name if name else 'datatypes',
            normalizer=normalizer,
            features=features,
            labels=labels
        )
