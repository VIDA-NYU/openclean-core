# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for FDViolation operator."""

from openclean.operator.collector.count import distinct
from openclean.operator.map.violations import fd_violations, key_violations, n_violations


def test_fdviolation_operator(agencies):
    """ Test fd violation works correctly"""
    lhs = ['borough', 'state']
    rhs = 'agency'
    fd = fd_violations(df=agencies, lhs=lhs, rhs=rhs)
    assert fd.get(('BK', 'NY')).shape[0] == 6
    assert fd.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') not in fd.keys()


def test_fdviolations_parking(parking):
    """Ensure that all returned fd violations for the parking tickets FD
    'Meter Number' -> 'Street' have at least two distinct values in the
    'Street' attribute.
    """
    # Get violations for the FD 'Meter Number' -> 'Street'
    groups = fd_violations(parking, lhs='Meter Number', rhs='Street')
    assert len(groups) > 0
    # For each group count the number of distinct values in the 'Street'
    # attribute.
    for key in groups:
        values = distinct(groups.get(key), 'Street')
        assert len(values) > 1

def test_keyviolations_parking(parking):
    """Ensure that there exist 3 duplicates in the 'Plate ID' column
    and 2 of them are at the same meter
    """
    groups = key_violations(parking, columns='Plate ID')
    assert len(groups) == 3

    groups = key_violations(parking, columns=['Plate ID', 'Meter Number'])
    assert len(groups) == 2

def test_nviolations_parking(parking):
    """Ensure we can select groups with exactly n=2 violations"""
    groups = n_violations(df=parking, columns='Plate ID', n=2)
    assert len(groups) == 3
