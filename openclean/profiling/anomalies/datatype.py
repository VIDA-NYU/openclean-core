# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for detecting values in a column that do not match the (expected)
data type for the column.
"""

from openclean.data.sequence import Sequence
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
    return op.find(values=set(Sequence(df=df, columns=columns)))


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
        super(DatatypeOutliers, self).__init__(name='datatypeOutlier')
        # Ensure that the domain is not a scalar value.
        if type(domain) in [int, float, str]:
            self.domain = set([domain])
        else:
            self.domain = domain
        self.classifier = classifier

    def outlier(self, value):
        """Use classifier to get the data type for the given value. If the
        returned type label is not included in the set of valid type labels
        the value is considered an outlier.

        Returns a dictionary for values that are classified as outliers that
        contains two elements: 'value' and 'label', containing the tested value
        and the returned type label, respectively.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        bool
        """
        type_label = self.classifier(value)
        if type_label not in self.domain:
            return {'value': value, 'label': type_label}
