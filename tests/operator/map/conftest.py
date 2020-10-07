# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame containing parking violations dataset."""

import os
import pandas as pd
import pytest

DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
FILENAME = os.path.join(DIR, 'parking-violations.tsv')


@pytest.fixture
def parking():
    return pd.read_csv(FILENAME, sep='\t', index_col=0)
