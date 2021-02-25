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

DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.files')
CONFIG_DIR = os.path.join(DIR, 'config')
DEFAULT_WORKERS = os.path.join(CONFIG_DIR, 'default.yaml')
PACKAGE_WORKERS = os.path.join(CONFIG_DIR, 'pckg.json')


def test_config_datadir():
    """Test getting the default data directory from the configuration settings
    in the environment.
    """
    os.environ[config.ENV_DATA_DIR] = '/my/dir'
    assert config.DATADIR() == '/my/dir'
    del os.environ[config.ENV_DATA_DIR]


def test_config_masterdatadir():
    """Test getting the default master data directory from the configuration
    settings in the environment.
    """
    os.environ[config.ENV_MASTERDATA_DIR] = '/my/masterdir'
    assert config.MASTERDATADIR() == '/my/masterdir'
    del os.environ[config.ENV_MASTERDATA_DIR]


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


def test_config_workers():
    """Test getting the flowServ worker factory from files referenced by
    ennvironment variables.
    """
    # Get the default configuration.
    os.environ[config.ENV_WORKERS] = DEFAULT_WORKERS
    factory = config.WORKERS()
    assert factory.config['test1']['worker'] == 'docker'
    assert factory.config['test2']['worker'] == 'subprocess'
    # Overwrite configuration with package specific settings.
    os.environ['OPENCLEAN_WORKERS_TEST'] = PACKAGE_WORKERS
    factory = config.WORKERS(var='OPENCLEAN_WORKERS_TEST')
    assert factory.config['test1']['worker'] == 'docker'
    assert factory.config['test2']['worker'] == 'docker'
    # By default the factory configuration is empty.
    del os.environ[config.ENV_WORKERS]
    del os.environ['OPENCLEAN_WORKERS_TEST']
    factory = config.WORKERS()
    assert factory.config == dict()
