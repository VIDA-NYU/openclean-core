# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for data type profiling."""

import pytest

from openclean.function.value.classifier import ValueClassifier
from openclean.function.value.datatype import Float, Int
from openclean.function.value.normalize import divide_by_total
from openclean.profiling.classifier.base import ResultFeatures
from openclean.profiling.classifier.datatype import datatypes, Datatypes


def test_datatype_profiler(employees, schools):
    """Compute counts for an input data frame."""
    # Types from employee salaries: 3 x int, 2 x float, 2 x string
    types = datatypes(employees, columns='Salary')
    assert len(types) == 3
    assert types['int'] == 3
    assert types['float'] == 2
    assert types['text'] == 2
    types = datatypes(employees, columns='Salary', normalizer=divide_by_total)
    assert len(types) == 3
    assert types['int'] == 3/7
    assert types['float'] == 2/7
    assert types['text'] == 2/7
    # Types from school level detail grades: 8 (30) x int, 1 (6) float, 4 (64) x string.
    classifier = ValueClassifier(Int(), Float(), default_label='str')
    types = datatypes(schools, columns='grade', classifier=classifier)
    assert len(types) == 3
    assert types['float'] == 6
    assert types['int'] == 30
    assert types['str'] == 64
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features=ResultFeatures.DISTINCT
    )
    assert len(types) == 3
    assert types['float'] == 1
    assert types['int'] == 8
    assert types['str'] == 4
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features=ResultFeatures.TOTAL,
        normalizer=divide_by_total
    )
    assert len(types) == 3
    assert types['float'] == 6/100
    assert types['int'] == 30/100
    assert types['str'] == 64/100
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features=ResultFeatures.BOTH
    )
    assert len(types) == 3
    assert types['float'] == {'distinct': 1, 'total': 6}
    assert types['int'] == {'distinct': 8, 'total': 30}
    assert types['str'] == {'distinct': 4, 'total': 64}
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features=ResultFeatures.BOTH,
        normalizer=divide_by_total,
        labels=['dist', 'ttl']
    )
    assert len(types) == 3
    assert types['float'] == {'dist': 1/13, 'ttl': 6/100}
    assert types['int'] == {'dist': 8/13, 'ttl': 30/100}
    assert types['str'] == {'dist': 4/13, 'ttl': 64/100}


def test_datatypes_error_cases():
    """Test error cases for datypes profiling."""
    # Invalid features value
    with pytest.raises(TypeError):
        Datatypes(features='unknown')
    # Invalid label list
    with pytest.raises(ValueError):
        Datatypes(features=ResultFeatures.BOTH, labels=['only-one'])
