# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the configuration helper functions."""

import os
import pytest

import openclean.config as config


def test_config_datadir():
    """Test getting the default data directory from the configuration settings
    in the environment.
    """
    os.environ[config.ENV_DATA_DIR] = '/my/dir'
    assert config.DATADIR() == '/my/dir'
    del os.environ[config.ENV_DATA_DIR]


@pytest.mark.parametrize(
    'value,result',
    [('a', 1), ('-34.5', 1), ('0', 1), ('1', 1), ('4', 4)]
)
def test_config_threads(value, result):
    """Test getting the number of threads for parallel processing from the
    configuration settings in the environment.
    """
    os.environ[config.ENV_THREADS] = value
    assert config.THREADS() == result
    del os.environ[config.ENV_THREADS]
