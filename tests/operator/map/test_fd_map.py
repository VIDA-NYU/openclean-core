# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for FDViolation operator."""

from openclean.function.distinct import distinct
from openclean.operator.map.fd import fd_violations, FDViolations


def test_fdviolation_operator(agencies):
    """ Test fd violation works correctly"""
    fd = FDViolations(
        lhs=['borough', 'state'],
        rhs='agency'
    ).map(agencies)
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
