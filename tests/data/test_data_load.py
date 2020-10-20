# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the dataset loader."""

import os

from openclean.data.load import dataset
from openclean.profiling.datatype.convert import DefaultConverter


DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.files')
SCHOOLS_FILE = os.path.join(DIR, 'school_level_detail.csv')


def test_load_and_convert():
    """Test loading a dataset from file when using a datatype converter."""
    df = dataset(SCHOOLS_FILE, typecast=DefaultConverter())
    assert list(df.iloc[0]) == ['X', 'X018', 2, 23, 23, 23]
