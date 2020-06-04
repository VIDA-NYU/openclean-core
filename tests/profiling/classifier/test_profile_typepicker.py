# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for data type assignment."""

from openclean.function.value.base import PreparedFunction
from openclean.function.value.classifier import ValueClassifier
from openclean.profiling.classifier.typepicker import (
    majority_typepicker, threshold_typepicker
)


class RoundRobinClassLabel(PreparedFunction):
    """Classifier that assigns labels 'A' and 'B' in round robin manner."""
    def __init__(self):
        self.is_a = True

    def eval(self, value):
        label = 'A' if self.is_a else 'B'
        self.is_a = not self.is_a
        return label


def test_majority_type_picker(schools):
    """Test picking the most frequenct type in a column."""
    types = majority_typepicker(schools, columns='grade')
    assert types == {'int': 8/13}
    types = majority_typepicker(schools, columns='grade', threshold=0.1)
    assert types == {'int': 8/13}
    types = majority_typepicker(schools, columns='grade', threshold=0.7)
    assert types == {}
    types = majority_typepicker(
        schools,
        columns='grade',
        use_total_counts=True
    )
    assert types == {'text': 70/100}
    # Use fake classifier to simulate scenario where more than one type
    # has maximum frequency.
    classifier = ValueClassifier(RoundRobinClassLabel())
    types = majority_typepicker(
        schools,
        columns='school_code',
        classifier=classifier
    )
    assert types == {'A': 0.5, 'B': 0.5}
    types = majority_typepicker(
        schools,
        columns='school_code',
        classifier=classifier,
        threshold=0.7
    )
    assert types == {}
    types = majority_typepicker(
        schools,
        columns='school_code',
        classifier=classifier,
        at_most_one=True
    )
    assert types == {}


def test_threshold_type_picker(schools):
    """Test picking data types in a column that satisfy a frequency
    constraint.
    """
    # Use distinct value counts as type frequencies.
    types = threshold_typepicker(schools, columns='grade', threshold=0.7)
    assert types == {}
    types = threshold_typepicker(schools, columns='grade', threshold=0.6)
    assert types == {'int': 8/13}
    types = threshold_typepicker(schools, columns='grade', threshold=0.1)
    assert types == {'int': 8/13, 'text': 5/13}
    types = threshold_typepicker(schools, columns='grade')
    assert types == {'int': 8/13, 'text': 5/13}
    # Use total value counts as type frequencies.
    types = threshold_typepicker(
        schools, columns='grade', threshold=0.8, use_total_counts=True)
    assert types == {}
    types = threshold_typepicker(
        schools,
        columns='grade',
        threshold=0.6,
        use_total_counts=True
    )
    assert types == {'text': 70/100}
    types = threshold_typepicker(
        schools,
        columns='grade',
        threshold=0.1,
        use_total_counts=True
    )
    assert types == {'int': 30/100, 'text': 70/100}
    types = threshold_typepicker(
        schools,
        columns='grade',
        use_total_counts=True
    )
    assert types == {'int': 30/100, 'text': 70/100}
