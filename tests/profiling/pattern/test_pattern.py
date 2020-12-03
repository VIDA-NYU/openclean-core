# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the pattern class."""


from openclean.profiling.pattern import OpencleanPatternFinder, IsNotMatch, IsMatch


def test_pattern_compare(nyc311):
    """test that the patternfinder class generates patterns from values"""
    dpf = OpencleanPatternFinder()

    patterns = dpf.find(nyc311['street'])
    assert len(patterns) == 6

    for pat in patterns.values():
        if len(pat) == 3: # ALPHA SPACE ALPHA
            oc_pattern = pat
            break

    matched_value = 'EBONY COURT'
    mismatched_value = '86 STREET'

    # using the pattern compare method
    assert oc_pattern.compare(value=matched_value, generator=dpf)
    assert not oc_pattern.compare(value=mismatched_value, generator=dpf)


def test_pattern_compile(nyc311):
    """Tests a pattern using the Value Function
    """
    pf = OpencleanPatternFinder()

    patterns = pf.find(nyc311['street'])
    assert len(patterns) == 6


    for pat in patterns.values():
        if len(pat) == 3: # ALPHA SPACE ALPHA
            oc_pattern = pat
            break

    matched_value = 'EBONY COURT'
    mismatched_value = '86 STREET'

    assert oc_pattern.compile(generator=pf).eval(matched_value)
    assert not oc_pattern.compile(generator=pf).eval(mismatched_value)


def test_func_match():
    """Test functionality of the match operator."""

    ROWS = [['32A West Broadway 10007'],
            ['54E East Village 10003']]

    pf = OpencleanPatternFinder()

    pattern = pf.find(series=ROWS[0])[7]

    match = ROWS[1][0]
    mismatch = '321-West Broadway 10007'

    # -- IsMatch --------------------------------------------------------------
    f = IsMatch(func=pattern.compare, generator=pf)
    assert f(match)
    assert not f(mismatch)

    # -- IsNotMatch -----------------------------------------------------------
    f = IsNotMatch(func=pattern.compare, generator=pf)
    assert not f(match)
    assert f(mismatch)