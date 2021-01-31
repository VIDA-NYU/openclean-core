# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for scikit-learn's outlier detection operators."""

from collections import Counter

from openclean.profiling.anomalies.sklearn import (
    dbscan, isolation_forest, local_outlier_factor, one_class_svm,
    robust_covariance
)


def test_sklearn_outliers_for_data_frame(nyc311):
    """Test scikit-learn's outlier detection operators for a column in a
    data frame.
    """
    ensemble = Counter()
    outliers = dbscan(nyc311, 'street')
    assert '5 AVENUE' in outliers
    ensemble.update(outliers)
    outliers = isolation_forest(nyc311, 'street')
    assert '5 AVENUE' in outliers
    ensemble.update(outliers)
    outliers = local_outlier_factor(nyc311, 'street')
    assert '5 AVENUE' in outliers
    ensemble.update(outliers)
    outliers = one_class_svm(nyc311, 'street')
    assert '5 AVENUE' in outliers
    ensemble.update(outliers)
    outliers = robust_covariance(nyc311, 'street')
    assert len(ensemble) > 0
    ensemble.update(outliers)
