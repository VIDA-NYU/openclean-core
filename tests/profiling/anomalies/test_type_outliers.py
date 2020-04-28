# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for detecting data type outliers."""

from openclean.profiling.anomalies import datatype_outliers

from openclean.function.classifier.base import ValueClassifier
from openclean.function.predicate.scalar.type import IsFloat, IsInt
from openclean.function.predicate.scalar import Gt


def test_datatype_outliers(schools):
    """Test detecting values that belong to minority data types in a column."""
    classifier = ValueClassifier(
        classifier=[IsInt(), IsFloat()],
        labels=['int', 'float'],
        default_label='str'
    )
    outliers = datatype_outliers(schools, 'grade', classifier, Gt(0.5))
    assert len(outliers) == 5
