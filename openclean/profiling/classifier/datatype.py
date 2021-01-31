# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes data type statistics and raw type domain
assignments for columns in a data frame.
"""

from typing import Callable, List, Optional, Tuple, Union

import pandas as pd

from openclean.function.eval.base import InputColumn
from openclean.function.value.base import ValueFunction
from openclean.function.value.classifier import ValueClassifier
from openclean.function.value.datatype import DefaultDatatypeClassifier
from openclean.profiling.classifier.base import Classifier, ResultFeatures


def datatypes(
    df: pd.DataFrame,
    columns: Union[InputColumn, List[InputColumn]] = None,
    classifier: Optional[ValueClassifier] = None,
    normalizer: Optional[Union[Callable, ValueFunction]] = None,
    features: Optional[ResultFeatures] = None,
    labels: Optional[Union[List[str], Tuple[str, str]]] = None
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
    columns: int, string, list, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a a single column reference or a list of column references.
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

    Returns
    -------
    dict
    """
    return Datatypes(
        classifier=classifier,
        normalizer=normalizer,
        features=features,
        labels=labels
    ).run(df=df, columns=columns)


class Datatypes(Classifier):
    """Compute data type frequency counts for values in a given list."""
    def __init__(
        self, classifier: Optional[ValueClassifier] = None,
        normalizer: Optional[Union[Callable, ValueFunction]] = None,
        features: Optional[ResultFeatures] = None,
        labels: Optional[Union[List[str], Tuple[str, str]]] = None
    ):
        """Initialize the associated classifier and optional normalizer.

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
            classifier = DefaultDatatypeClassifier()
        super(Datatypes, self).__init__(
            classifier=classifier,
            normalizer=normalizer,
            features=features,
            labels=labels
        )
