# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for detecting values in a column that do not match the (expected)
data type for the column.
"""

from collections import defaultdict
from typing import Callable, Dict, List, Union

import pandas as pd

from openclean.data.types import Scalar, Value
from openclean.function.eval.base import InputColumn
from openclean.profiling.anomalies.conditional import ConditionalOutliers


# -- Outlier results ----------------------------------------------------------

class DatatypeOutlierResults(list):
    """Datatype outlier results are a list of dictionaries. Each dictionary
    contains information about a detected outlier value ('value') and additional
    metadata ('metadata': {'type'}) about the assigned type label for the
    outlier value.

    This class provides some basic functionality to access the individual
    pieces of information from these dictionaries.
    """

    def types(self) -> Dict:
        """Get a mapping of outlier types to a list of values of that type.

        Returns
        -------
        dict

        Raises
        ------
        KeyError
        """
        types = defaultdict(list)
        for o in self:
            types[o['metadata']['type']].append(o['value'])
        return types

    def values(self) -> List:
        """Get only the list of outlier vaues.

        Returns
        -------
        list
        """
        return [o['value'] for o in self]


# -- Datatype outlier detection operators -------------------------------------

def datatype_outliers(
    df: pd.DataFrame, columns: Union[InputColumn, List[InputColumn]],
    classifier: Callable, domain: Union[Scalar, List[Scalar]]
) -> Dict:
    """Identify values that do not match the expected data type. The expected
    data type for a (list of) column(s) is defined by the given domain. The
    classifier is used to identify the type of data values. Values that are
    assigned a type that does not belong to the defined domain are considered
    data type outliers.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, list, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a a single column reference or a list of column references.
    classifier: callable
        Classifier that assigns data type class labels to column values.
    domain: scalar or list
        Valid data type value(s). Defines the types that are not considered
        outliers.

    Returns
    -------
    dict
    """
    return DatatypeOutliers(
        classifier=classifier,
        domain=domain
    ).run(df=df, columns=columns)


class DatatypeOutliers(ConditionalOutliers):
    """Identify values that do not match the expected data type for a list of
    values (e.g., a column in a data frame). The expected data type is defined
    by a set of data type labels. A classifier is used to identify the type of
    values. Values that are assigned a type that are not included in the set of
    expected type labels are considered outliers.
    """
    def __init__(
        self, classifier: Callable, domain: Union[Scalar, List[Scalar]]
    ):
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
        super(DatatypeOutliers, self).__init__(resultcls=DatatypeOutlierResults)
        # Ensure that the domain is not a scalar value.
        if type(domain) in [int, float, str]:
            self.domain = set([domain])
        else:
            self.domain = domain
        self.classifier = classifier

    def outlier(self, value: Value) -> Dict:
        """Use classifier to get the data type for the given value. If the
        returned type label is not included in the set of valid type labels
        the value is considered an outlier.

        Returns a dictionary for values that are classified as outliers that
        contains two elements: 'value' and 'metadata', containing the tested
        value and the returned type label (in the 'metadata' dictionary with
        key 'type'), respectively.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        dict
        """
        type_label = self.classifier(value)
        if type_label not in self.domain:
            return {'value': value, 'metadata': {'type': type_label}}
