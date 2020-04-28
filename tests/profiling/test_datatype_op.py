# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.function.classifier.base import ValueClassifier
from openclean.data.stream import Stream
from openclean.profiling import datatypes, distinct
from openclean.function.predicate.scalar.type import IsFloat, IsInt


def test_cardinality_profiler(employees, schools):
    """Compute counts for an input data frame."""
    classifier = ValueClassifier(
        classifier=[IsInt(), IsFloat()],
        labels=['int', 'float'],
        default_label='str'
    )
    # Types from employee salaries: 3 x int, 2 x float, 2 x string
    types = datatypes(df=employees, columns='Salary', classifier=classifier)
    assert len(types) == 3
    assert types['int']['distinct'] == 3
    assert types['int']['total'] == 3
    assert types['float']['distinct'] == 2
    assert types['float']['total'] == 2
    assert types['str']['distinct'] == 2
    assert types['str']['total'] == 2
    # Types from school level detail grades: 8 (30) x int, 5 (70) x string
    types = datatypes(schools, 'grade', classifier, label_distinct='count')
    assert types['int']['count'] == 8
    assert types['int']['total'] == 30
    assert types['str']['count'] == 5
    assert types['str']['total'] == 70
    # Use a value stream
    types = datatypes(classifier=classifier, values=Stream(schools, 'grade'))
    assert types['int']['distinct'] == 8
    assert types['int']['total'] == 30
    assert types['str']['distinct'] == 5
    assert types['str']['total'] == 70
    # Use a distinct value set with counts
    dist_counts = distinct(schools, 'grade')
    types = datatypes(classifier=classifier, distinct_counts=dist_counts)
    assert types['int']['distinct'] == 8
    assert types['int']['total'] == 30
    assert types['str']['distinct'] == 5
    assert types['str']['total'] == 70
    # Invalid argument combinations
    with pytest.raises(ValueError):
        datatypes(schools, 'grade', classifier, values='count')
    with pytest.raises(ValueError):
        datatypes(schools, classifier, values=Stream(schools, 'grade'))
    with pytest.raises(ValueError):
        datatypes(schools, classifier, distinct_counts=dist_counts)
