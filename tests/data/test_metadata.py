# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for data structures that maintain metadata about a list of
objects.
"""

from openclean.data.metadata import Feature, FeatureDict, FeatureVector


def test_metadata_array():
    """Test initializing the metadata array class."""
    # Scalar value array
    values = Feature({'A': 1, 'B': 2, 'C': 3})
    assert sorted(values.keys()) == ['A', 'B', 'C']
    val = 1
    for key in ['A', 'B', 'C']:
        assert values[key] == val
        assert values.get(key) == val
        assert values.value(key, 'count') == val
        val += 1
    # Unknown labels and error conditions
    assert values.value('A', 'v') is None
    assert values.value('A', 'v', default_value=0) == 0
    assert values.value('D', 'count') is None
    assert values.value('D', 'v', default_value=1) == 1
    # Convert to dictionary
    metadict = values.to_dict('count')
    assert metadict['A'] == {'count': 1}
    assert metadict['B'] == {'count': 2}
    assert metadict['C'] == {'count': 3}
    # Convert the object to a numpy array
    data = values.data
    assert data.shape == (3, )
    # Tuple value array
    values = Feature({('A', 3): 1, ('B', 2): 2, ('C', 1): 3}, label='v')
    assert sorted(values.keys()) == [('A', 3), ('B', 2), ('C', 1)]
    val = 1
    for key in ['A', 'B', 'C']:
        key = (key, 4 - val)
        assert values[key] == val
        assert values.get(key) == val
        assert values.value(key, 'v') == val
        val += 1


def test_metadata_dict():
    """Test creating a metadata dictionary."""
    metadata = FeatureDict()
    for key in ['A', 'B', 'C']:
        metadata[key]['count'] = len(metadata) + 1
    assert metadata['A'] == {'count': 1}
    assert metadata['B'] == {'count': 2}
    assert metadata['C'] == {'count': 3}
    # Get a metadata array for the count feature.
    metadata['A']['total'] = 10
    meta_array = metadata.to_scalar('count')
    assert meta_array['A'] == 1
    assert meta_array['B'] == 2
    assert meta_array['C'] == 3
    # Entries with missing values.
    meta_array = metadata.to_scalar('total')
    assert meta_array['A'] == 10
    assert meta_array['B'] is None
    assert meta_array['C'] is None
    # Entries with missing values. No default value.
    meta_array = metadata.to_scalar('total')
    assert meta_array['A'] == 10
    assert meta_array['B'] is None
    assert meta_array['C'] is None
    # Entries with missing values. Use default value.
    meta_array = metadata.to_scalar('total', default_value=5)
    assert meta_array['A'] == 10
    assert meta_array['B'] == 5
    assert meta_array['C'] == 5


def test_feature_vector():
    """Test creating multi-dimensional feature vectors."""
    # Ensure that the order of values in preserved as they are inserted.
    vec = FeatureVector()
    vec.add('Z', [0, 1])
    vec.add('H', [2, 3])
    vec.add('A', [4, 5])
    assert list(vec.keys()) == ['Z', 'H', 'A']
    data = vec.data
    assert data.shape == (3, 2)
    assert list(data[0]) == [0, 1]
    assert list(data[1]) == [2, 3]
    assert list(data[2]) == [4, 5]
    # Changing an existing value should not change the order of values.
    vec.add('H', [6, 7])
    assert list(vec.keys()) == ['Z', 'H', 'A']
    data = vec.data
    assert data.shape == (3, 2)
    assert list(data[0]) == [0, 1]
    assert list(data[1]) == [6, 7]
    assert list(data[2]) == [4, 5]
