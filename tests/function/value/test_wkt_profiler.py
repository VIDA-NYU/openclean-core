# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the WKT type parser."""

from openclean.function.value.geo import WKTType, is_wkt


def test_wkt_type_label():
    """Test type classifier function."""
    # Default behavior.
    f = WKTType()
    assert f.eval('POINT (30 10)') == 'wkt:Point'
    assert f.eval('POLYGON ((30 10, 40 40, 20 40, 30 10))') == 'wkt:Polygon'
    assert f.eval('Well known text') is None
    # Exclude type from label.
    f = WKTType(include_type=False)
    assert f.eval('POINT (30 10)') == 'wkt'
    assert f.eval('POLYGON ((30 10, 40 40, 20 40, 30 10))') == 'wkt'
    assert f.eval(2) is None
    # Use a different delimiter to concat labels and a different none-label.
    f = WKTType(include_type=True, delim='-', none_label='no wkt')
    assert f.eval('POINT (30 10)') == 'wkt-Point'
    assert f.eval('POLYGON ((30 10, 40 40, 20 40, 30 10))') == 'wkt-Polygon'
    assert f.eval(None) == 'no wkt'


def test_wkt_type_predicate():
    """Test predicate that is used to check whether a value is in WKT
    representation or not.
    """
    # Use positive examples from
    # https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
    assert is_wkt('POINT (30 10)')
    assert is_wkt('LINESTRING (30 10, 10 30, 40 40)')
    assert is_wkt('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')
    assert is_wkt(
        'POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), '
        '(20 30, 35 35, 30 20, 20 30))'
    )
    assert is_wkt('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')
    assert is_wkt('MULTIPOINT (10 40, 40 30, 20 20, 30 10)')
    assert is_wkt(
        'MULTILINESTRING ((10 10, 20 20, 10 40),'
        '(40 40, 30 30, 40 20, 30 10))'
    )
    assert is_wkt(
        'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),'
        '((15 5, 40 10, 10 20, 5 10, 15 5)))'
    )
    assert is_wkt(
        'MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),'
        '((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),'
        '(30 20, 20 15, 20 25, 30 20)))'
    )
    assert is_wkt(
        'GEOMETRYCOLLECTION (POINT (40 10),'
        'LINESTRING (10 10, 20 20, 10 40),'
        'POLYGON ((40 40, 20 45, 45 30, 40 40)))'
    )
    # Some negative examples
    assert not is_wkt('Well known text')
    assert not is_wkt('(30, 10)')
    assert not is_wkt('POINT (30, 10)')
    assert not is_wkt('POLYGON ((30 10, 10 20, 30 10))')
    assert not is_wkt(2)
    assert not is_wkt(None)
