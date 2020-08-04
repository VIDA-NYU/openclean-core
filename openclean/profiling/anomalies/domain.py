# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Domain outlier detector."""

from openclean.function.value.domain import IsNotInDomain
from openclean.profiling.anomalies.conditional import ConditionalOutliers
from openclean.profiling.base import profile


def domain_outliers(df, columns, domain, ignore_case=False):
    """The domain outlier detector returns the list of values from a given data
    stream that do not occur in a ground truth domain.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    domain: pandas.DataFrame, pandas.Series, or object
        Data frame or series, or any object that implements the
        __contains__ method.
    ignore_case: bool, optional
        Ignore case for domain inclusion checking

    Returns
    -------
    list
    """
    op = DomainOutliers(domain=domain, ignore_case=ignore_case)
    return list(profile(df, columns=columns, profilers=op)[op.name].keys())


class DomainOutliers(ConditionalOutliers):
    """The domain outlier detector returns the list of values from a given data
    stream that do not occur in a ground truth domain.
    """
    def __init__(self, domain, ignore_case=False):
        """Initialize the ground truth domain.

        Parameters
        ----------
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, optional
            Ignore case for domain inclusion checking
        """
        super(DomainOutliers, self).__init__(name='domainOutlier')
        self.predicate = IsNotInDomain(domain=domain, ignore_case=ignore_case)

    def outlier(self, value):
        """Test if a given value is in the associated ground truth domain. If
        the value is not in the domain it is considered an outlier.

        Returns a dictionary for values that are classified as outliers that
        contains one element 'value' for the tested value.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        bool
        """
        if self.predicate.eval(value):
            return True
