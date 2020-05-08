# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for detecting values in a column that do not match the (expected)
data type for the column.
"""

from openclean.data.stream import Stream
from openclean.profiling.anomalies.conditional import ConditionalOutliers


def datatype_outliers(df, columns, classifier, domain):
    """Identify values that do not match the expected data type. The expected
    data type for a (list of) column(s) is defined by the given domain. The
    classifier is used to identify the type of data values. Values that are
    assigned a type that does not belong to the defined domain are considered
    data type outliers.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    classifier: callable
        Classifier that assigns data type class labels to column values.
    domain: scalar or list
        Valid data type value(s). Defines the types that are not considered
        outliers.

    Returns
    -------
    list
    """
    op = DatatypeOutliers(classifier=classifier, domain=domain)
    return op.find(values=set(Stream(df=df, columns=columns)))


class DatatypeOutliers(ConditionalOutliers):
    """Identify values that do not match the expected data type for a list of
    values (e.g., a column in a data frame). The expected data type is defined
    by a set of data type labels. A classifier is used to identify the type of
    values. Values that are assigned a type that are not included in the set of
    expected type labels are considered outliers.
    """
    def __init__(self, classifier, domain):
        """Initialize the classifier that is used to assign type labels to data
        values and the domain of expected (valid) type labels.

        Parameters
        ----------
        classifier: callable
            Classifier that assigns data type class labels to column values.
        domain: scalar or list
            Valid data type value(s). Defines the types that are not considered
            outliers.
        """
        # Ensure that the domain is not a scalar value.
        if type(domain) in [int, float, str]:
            self.domain = set([domain])
        else:
            self.domain = domain
        self.classifier = classifier
        # Use a predicate that takes the given classifier to determine the data
        # type of a value and then checks whether that type belogs to the list
        # of valid types or not.
        super(DatatypeOutliers, self).__init__(predicate=self.is_outlier)

    def is_outlier(self, value):
        """Use classifier to get the data type for the given value. Return True
        if the returned type label is not included in the set of valid type
        labels.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        bool
        """
        return self.classifier(value) not in self.domain
