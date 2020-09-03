# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the fuzzy string matching."""

from openclean.profiling.matcher.fuzzy import FuzzyStringMatcher

def test_match(zipcodes):
    """" Test match works correctly """
    masterdata = ['Brooklyn','Manhattan','Queens','Staten Island','Bronx']
    fsm = FuzzyStringMatcher(data=masterdata)
    matches = fsm.match(zipcodes['Borough'])
    assert {'Brooklyn', 'Manhattan', 'Queens', 'Broooklyn', 'Quees'} == set(matches.results.keys())

