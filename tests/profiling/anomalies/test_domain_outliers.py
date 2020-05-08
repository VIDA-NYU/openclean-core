# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the domain outlier detection."""

from openclean.profiling.anomalies.domain import (
    domain_outliers, DomainOutliers
)


def test_domain_outlier_detection_in_list():
    """Test finding outliers for a given ground truth domain in a given list of
    values.
    """
    domain = set({'A', 'B', 'C'})
    # -- Find outliers case sensitive -----------------------------------------
    op = DomainOutliers(domain=domain)
    outliers = op.find(['A', 'b', 'D'])
    assert set(outliers) == set({'b', 'D'})
    # -- Find outliers case insensitive ---------------------------------------
    op = DomainOutliers(domain=domain, ignore_case=True)
    outliers = op.find(['A', 'b', 'D'])
    assert outliers == ['D']


def test_domain_outlier_detection_in_data_frame(employees):
    """Test finding domain outliers in a column of a data frame."""
    domain = list(range(30, 40))
    # -- Find outliers case sensitive -----------------------------------------
    outliers = domain_outliers(df=employees, columns='Age', domain=domain)
    assert len(outliers) == 4
    # -- Find outliers case insensitive ---------------------------------------
    outliers = domain_outliers(
        df=employees,
        columns='Age',
        domain=domain,
        ignore_case=True
    )
    assert len(outliers) == 4
