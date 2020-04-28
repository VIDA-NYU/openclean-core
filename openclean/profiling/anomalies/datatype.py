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


# -- Operator -----------------------------------------------------------------

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
    # Ensure that the domain is not a scalar value.
    if type(domain) in [int, float, str]:
        domain = set([domain])
    # Create a predicate that uses the classifier to determine the data type
    # of a value and checks whether that type belogs to the list of valid
    # types or not.

    def predicate(value):
        """Returns true if the data type assigned to the value does not belong
        to the domain of valid data types.
        """
        return classifier(value) not in domain

    op = ConditionalOutliers(predicate=predicate)
    return op.predict(values=set(Stream(df=df, columns=columns)))
