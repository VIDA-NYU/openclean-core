# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column profiler."""

from openclean.profiling.column import (
    DefaultColumnProfiler, DefaultStreamProfiler
)


def test_profile_single_column(schools):
    """Test profiling a single data frame column using the default profiler."""
    # -- Use default labels for result ----------------------------------------
    metadata = DefaultColumnProfiler(top_k=3).run(schools, 'grade')
    assert metadata['minimumValue'] == '01'
    assert metadata['maximumValue'] == 'MS Core'
    assert metadata['totalValueCount'] == 100
    assert metadata['emptyValueCount'] == 6
    assert metadata['distinctValueCount'] == 12
    assert metadata['datatypes'] == {
        'int': {'distinct': 8, 'total': 30},
        'text': {'distinct': 4, 'total': 64}
    }
    assert metadata['topValues'] == [
        ("09-12", 38),
        ("MS Core", 21),
        ("07", 8)
    ]
    # -- Use custom labels for result -----------------------------------------
    metadata = DefaultColumnProfiler(
        label_datatypes='types',
        label_distinct_values='distinct',
        label_empty_count='empty',
        label_min='min',
        label_max='max',
        label_top_values='topk',
        label_total_count='total'
    ).run(schools, 'grade')
    assert metadata['min'] == '01'
    assert metadata['max'] == 'MS Core'
    assert metadata['total'] == 100
    assert metadata['empty'] == 6
    assert metadata['distinct'] == 12
    assert metadata['types'] == {
        'int': {'distinct': 8, 'total': 30},
        'text': {'distinct': 4, 'total': 64}
    }
    assert len(metadata['topk']) == 10


def test_profile_single_column_stream(schools):
    """Test profiling a data stream for a single dataset column using the
    default stream profiler.
    """
    # -- Use default labels for result ----------------------------------------
    metadata = DefaultStreamProfiler().run(schools, 'grade')
    assert metadata['minimumValue'] == '01'
    assert metadata['maximumValue'] == 'MS Core'
    assert metadata['totalValueCount'] == 100
    assert metadata['emptyValueCount'] == 6
    assert metadata['datatypes'] == {'int':  30, 'text': 64}
    # -- Use custom labels for result -----------------------------------------
    metadata = DefaultStreamProfiler(
        label_datatypes='types',
        label_empty_count='empty',
        label_min='min',
        label_max='max',
        label_total_count='total'
    ).run(schools, 'grade')
    assert metadata['min'] == '01'
    assert metadata['max'] == 'MS Core'
    assert metadata['total'] == 100
    assert metadata['empty'] == 6
    assert metadata['types'] == {'int':  30, 'text': 64}
