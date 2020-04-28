# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for column statistics operator."""

import statistics
import os

from openclean.data.load import dataset
from openclean.profiling import Stats, colstats
from openclean.function.feature import Length, DigitsCount, WhitespaceCount


DIR = os.path.dirname(os.path.realpath(__file__))
CSV_FILE = os.path.join(DIR, '../data/school_level_detail.csv')


def test_column_stats_profiling():
    """Test computing statistics for all columns in a data frame."""
    ds = dataset(CSV_FILE)
    # Statistics functions
    st = [min, max, statistics.mean, statistics.stdev]
    fstats = [
        Stats(Length(), st),
        Stats(DigitsCount(), st),
        Stats(WhitespaceCount(), st)
    ]
    stats = colstats(ds, fstats)
    assert stats.shape == (12, 6)
