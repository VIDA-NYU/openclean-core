# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the patternfinder class."""

from openclean.profiling.pattern.ocpattern import OpencleanPatternFinder
from openclean_pattern.datatypes.base import SupportedDataTypes


def test_patternfinder_compare(nyc311):
    """test that the patternfinder objects can compare patterns and values"""
    dpf = OpencleanPatternFinder()

    patterns = dpf.find(nyc311['street'])
    assert len(patterns) == 6

    for pat in patterns.values():
        if len(pat) == 3: # ALPHA SPACE ALPHA
            oc_pattern = pat
            break

    matched_value = 'EBONY COURT'
    mismatched_value = '86 STREET'

    # using the patternfinder compare method
    assert dpf.compare(oc_pattern, matched_value)
    assert not dpf.compare(oc_pattern, mismatched_value)


def test_patterfinder_find(nyc311):
    """test that the patternfinder objects generate patterns from values"""
    dpf = OpencleanPatternFinder()

    patterns = dpf.find(nyc311['street'])
    assert len(patterns) == 6
    assert patterns[1][0].element_type == SupportedDataTypes.ALPHA.name
    assert patterns[7][0].element_type == SupportedDataTypes.ALPHA.name
    assert patterns[7][1].element_type == SupportedDataTypes.SPACE_REP.name
    assert patterns[7][2].element_type == SupportedDataTypes.ALPHA.name
    assert patterns[7][3].element_type == SupportedDataTypes.SPACE_REP.name
    assert patterns[7][4].element_type == SupportedDataTypes.ALPHA.name
    assert patterns[7][5].element_type == SupportedDataTypes.SPACE_REP.name
    assert patterns[7][6].element_type == SupportedDataTypes.ALPHA.name