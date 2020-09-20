# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for FDViolation operator."""

from openclean.operator.map.fd import FDViolations

def test_fdviolation_operator(agencies):
    """ Test fd violation works correctly"""
    fd = FDViolations(
        lhs=['borough','state'],
        rhs='agency'
    ).map(agencies)
    assert fd.get(('BK', 'NY')).shape[0] == 6
    assert fd.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') not in fd.keys()
