# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for dataset downloader descriptors."""

import pytest

from jsonschema import ValidationError

from openclean.data.downloader.base import (
    ColumnDescriptor, DatasetDescriptor
)


def test_column_descriptor():
    """Test column descriptor wrapper."""
    col = ColumnDescriptor({'name': 'col1', 'description': 'Column 1'})
    assert col.name() == 'col1'
    assert col.description() == 'Column 1'
    col = ColumnDescriptor({'name': 'col2'})
    assert col.name() == 'col2'
    assert col.description() == ''
    col = ColumnDescriptor({'name': 'col1', 'descriptor': 'Column 1'})
    col.description() == ''
    with pytest.raises(ValidationError):
        ColumnDescriptor(
            {'name': 'col1', 'description': 1},
            validate=True
        )


def test_dataset_descriptor():
    """Test dataset descriptor wrapper."""
    # Complete descriptor
    ds = DatasetDescriptor(doc={
        'id': '0000',
        'name': 'Dataset name',
        'description': 'Dataset description',
        'columns': [
            {'name': 'col1', 'description': 'Column 1'},
            {'name': 'col2', 'description': 'Column 2'}
        ]
    })
    assert ds.identifier() == '0000'
    assert ds.name() == 'Dataset name'
    assert ds.description() == 'Dataset description'
    assert len(ds.columns()) == 2
    col1 = ds.columns(0)
    assert col1.name() == 'col1'
    assert col1.description() == 'Column 1'
    # Minimal descriptor
    ds = DatasetDescriptor(doc={
        'id': '0001',
        'columns': []
    })
    assert ds.identifier() == '0001'
    assert ds.name() == '0001'
    assert ds.description() == ''
    assert len(ds.columns()) == 0
    # Error case
    with pytest.raises(ValidationError):
        ds = DatasetDescriptor(doc={'id': '0001', 'column': []})
    ds = DatasetDescriptor(doc={'id': '0001', 'column': []}, validate=False)
    with pytest.raises(KeyError):
        ds.columns()
