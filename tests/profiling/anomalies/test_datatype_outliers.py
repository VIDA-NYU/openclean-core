# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for detecting data type outliers."""

from openclean.profiling.anomalies.datatype import (
    datatype_outliers, DatatypeOutliers
)

from openclean.function.classifier import ValueClassifier
from openclean.function.value.datatype import is_float, is_int
from openclean.function.value.domain import is_in


def test_datatype_outliers_in_list():
    """Test finding data type outliers in a list of values."""
    classifier = ValueClassifier(
        classifier=[is_in(domain=list('ABCDE'))],
        labels=['grade'],
        default_label='unknown'
    )
    op = DatatypeOutliers(classifier=classifier, domain=['grade'])
    outliers = op.find(['A', 1, 'B', 'G'])
    assert set(outliers) == set({1, 'G'})


def test_datatype_outliers_in_data_frame(schools):
    """Test detecting values that belong to minority data types in a data frame
    column.
    """
    classifier = ValueClassifier(
        classifier=[is_int(), is_float()],
        labels=['number', 'number'],
        default_label='str'
    )
    outliers = datatype_outliers(
        df=schools,
        columns='grade',
        classifier=classifier,
        domain=['number']
    )
    assert set(outliers) == set({'', '09-12', 'MS Core', '0K-09', '0K'})
