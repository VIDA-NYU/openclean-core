# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for data type profiling."""

import pytest

from openclean.function.value.classifier import ValueClassifier
from openclean.function.value.datatype import Float, Int
from openclean.function.value.normalize import divide_by_total
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
    # Types from school level detail grades: 8 (30) x int, 5 (70) x string.
    classifier = ValueClassifier(Int(), Float(), default_label='str')
    types = datatypes(schools, columns='grade', classifier=classifier)
    assert types['int'] == 8
    assert types['str'] == 5
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features='total'
    )
    assert len(types) == 2
    assert types['int'] == 30
    assert types['str'] == 70
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features='total',
        normalizer=divide_by_total
    )
    assert len(types) == 2
    assert types['int'] == 30/100
    assert types['str'] == 70/100
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features='both'
    )
    assert len(types) == 2
    assert types['int'] == {'distinct': 8, 'total': 30}
    assert types['str'] == {'distinct': 5, 'total': 70}
    types = datatypes(
        schools,
        columns='grade',
        classifier=classifier,
        features='both',
        normalizer=divide_by_total,
        labels=['dist', 'ttl']
    )
    assert len(types) == 2
    assert types['int'] == {'dist': 8/13, 'ttl': 30/100}
    assert types['str'] == {'dist': 5/13, 'ttl': 70/100}


def test_datatypes_error_cases():
    """Test error cases for datypes profiling."""
    # Invalid features value
    with pytest.raises(ValueError):
        Datatypes(features='unknown')
    # Invalid label list
    with pytest.raises(ValueError):
        Datatypes(features='both', labels=['only-one'])
    # Manipulate features.
    dt = Datatypes(features='both')
    assert dt.map_distinct({'A': 1}) == {'text': {'distinct': 1, 'total': 1}}
    dt.features = 'unknown'
    with pytest.raises(ValueError):
        dt.map_distinct({'A': 1})
