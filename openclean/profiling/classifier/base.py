# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes statistics for a classification task on a
stream on data values.
"""

from openclean.data.metadata import FeatureDict, Feature
from openclean.data.sequence import Sequence


# -- Class profiling ----------------------------------------------------------

def classprofile(df, columns, classifier):
    """Compute class labels and label frequency counts for all values in the
    specified data frame column(s). Returns a dictionary with two elements:
    'values' is a dictionary that maps distinct column values to their assigned
    type labels; 'types' is a feature dictionary that maps each class label to
    counts of the dictinct number of values ('distinct') and the total number
    of values ('total') that were assigned the particular label.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int or string or list(int or string)
        List of column index or column name for columns for which distinct
        value combinations are computed.
    classifier: callable
        Callable that assigns class labels to values.

    Returns
    -------
    dict
    """
    values = dict()
    types = FeatureDict()
    for value in Sequence(df=df, columns=columns):
        if value not in values:
            label = classifier(value)
            types[label] = {'distinct': 1, 'total': 1}
            values[value] = label
        else:
            label = values[value]
            types[label]['total'] += 1
    return {'values': values, 'types': types}


# -- Label counts -------------------------------------------------------------

def classify(df, columns, classifier, distinct=False):
    """Create a single feature vector that counts for each label from the given
    clasifier the number of values in the specified data frame column(s) that
    were assigned that particular label. If the distinct flag is True only the
    distinct values in the column(s) will be counted.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int or string or list(int or string)
        List of column index or column name for columns for which distinct
        value combinations are computed.
    classifier: callable
        Callable that assigns class labels to values.
    distinct: bool, optional
        Consider only the set of distinct values in the data frame column(s).

    Returns
    -------
    openclean.data.metadata.Feature
    """
    values = Sequence(df=df, columns=columns)
    if distinct:
        values = set(values)
    return classify_distinct(values=values, classifier=classifier)


def classify_distinct(values, classifier):
    """Create a single feature vector that counts for each label from the given
    clasifier the number of values (in a stream of distinct values) that were
    assigned that particular label.

    Parameters
    ----------
    values: iterable
        Iterable stream of data values.
    classifier: callable
        Callable that assigns class labels to values.

    Returns
    -------
    openclean.data.metadata.Feature
    """
    result = Feature()
    for value in values:
        result[classifier(value)] += 1
    return result
