# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

from openclean.function.list.distinct import Distinct
from openclean.profiling.base import profile
from openclean.profiling.classifier.datatype import Datatypes


def test_profile_single_column(schools):
    """Test profiling a single columns with a data type and distinct value
    count profilier.
    """
    metadata = profile(
        schools,
        columns='grade',
        profilers=[Datatypes(features='both'), Distinct(name='distinct')]
    )
    assert 'datatypes' in metadata
    assert metadata['datatypes'] == {
        'int': {'distinct': 8, 'total': 30},
        'text': {'distinct': 5, 'total': 70}
    }
    assert 'distinct' in metadata
    assert len(metadata['distinct']) == 13
