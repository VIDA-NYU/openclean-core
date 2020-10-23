# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import os
import pandas as pd
import pytest

from openclean.data.types import Column
#from openclean.data.load import dataset


DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.files')
AGENCY_FILE = os.path.join(DIR, 'agencies.csv')
NYC311_FILE = os.path.join(DIR, '311-descriptor.csv')
SCHOOLS_FILE = os.path.join(DIR, 'school_level_detail.csv')


@pytest.fixture
def agencies():
    """List of agency names with NYC borough and US State."""
    return pd.read_csv(AGENCY_FILE)


@pytest.fixture
def dupcols():
    """Get a simple data frame with duplicate column names."""
    data = [
        ['Alice', 23, 180],
        ['Bob', 32, 179],
        ['Claudia', 37, 184],
        ['Dave', 45, 176],
        ['Eileen', 29, 168],
        ['Frank', 34, 198],
        ['Gertrud', 44, 177]
    ]
    columns = [Column(0, 'Name', 0), Column(1, 'A', 1), Column(2, 'A', 2)]
    return pd.DataFrame(data=data, columns=columns)


@pytest.fixture
def employees():
    """Get a simple data frame with the name, age, and salary of seven
    employees.
    """
    data = [
        ['Alice', 23, 60000],
        ['Bob', 32, ''],
        ['Claudia', 37, '21k'],
        ['Dave', None, 34567],
        ['Eileen', 29, 34598.87],
        ['Frank', 34, '23'],
        ['Gertrud', 44, '120050.5']
    ]
    columns = [Column(0, 'Name'), Column(1, 'Age'), Column(2, 'Salary')]
    return pd.DataFrame(data=data, columns=columns)


@pytest.fixture
def nyc311():
    """Load the 311 descriptor dataset."""
    return pd.read_csv(NYC311_FILE)


@pytest.fixture
def schools():
    """Load the school level detail dataset."""
    return pd.read_csv(SCHOOLS_FILE)
