# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

from openclean.function.value.datatype import is_int, is_float
from openclean.profiling.base import profile
from openclean.profiling.classifier.datatype import Datatypes
from openclean.profiling.count import Counts, Values


def test_profile_single_column(schools):
    """Test profiling a single columns with a data type and distinct value
    count profilier.
    """
    metadata = profile(
        schools,
        columns='grade',
        profilers=[
            Datatypes(features='both'),
            Values(name='distinct'),
            Counts(is_int, is_float)
        ]
    )
    assert len(metadata) == 3
    assert 'datatypes' in metadata
    assert metadata['datatypes'] == {
        'int': {'distinct': 8, 'total': 30},
        'text': {'distinct': 5, 'total': 70}
    }
    assert 'distinct' in metadata
    assert metadata['distinct'] == {'distinct': 13, 'total': 100}
    assert 'counts' in metadata
    assert metadata['counts'] == {'is_int': 30, 'is_float': 30}
    # Use normalized column count.
    metadata = profile(
        schools,
        columns='grade',
        profilers=[
            Datatypes(features='both'),
            Values(name='values'),
            Counts(is_int, is_float)
        ]
    )
    assert len(metadata) == 3
    assert 'datatypes' in metadata
    assert metadata['datatypes'] == {
        'int': {'distinct': 8, 'total': 30},
        'text': {'distinct': 5, 'total': 70}
    }
    assert 'values' in metadata
    assert metadata['values'] == {'distinct': 13, 'total': 100}
    assert 'counts' in metadata
    assert metadata['counts'] == {'is_int': 30, 'is_float': 30}
