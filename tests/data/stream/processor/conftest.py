# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for iterrows function of an empty data processing pipeline."""

import os
import pytest

from openclean.data.load import stream


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../.files')  # noqa: E501
NYC311_FILE = os.path.join(DIR, '311-descriptor.csv')


@pytest.fixture
def stream311():
    """Get stream processor for the 311 test data file."""
    return stream(NYC311_FILE)
