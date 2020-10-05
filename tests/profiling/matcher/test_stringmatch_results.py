# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string matching results class."""


from openclean.profiling.matcher.base import StringMatcherResults

def test_string_matcher_results(zipcodes):
    """" Test string matcher results class works correctly"""
    masterdata = ['Brooklyn','Manhattan','Queens','Staten Island','Bronx']
    smr = StringMatcherResults()
    stub_match = [(.1,'stub1'),(.4, 'stub2'),(.2, 'stub3')]

    for i, row in zipcodes.iterrows():
        if row['Borough'] in masterdata:
            smr.add(i, row['Borough'], [(1, row['Borough'])])
        else:
            smr.add(i, row['Borough'], stub_match)

    assert set(smr.get_mismatches().keys()) == {'Broooklyn','Quees'}
    assert smr.get_indices('Quees') == [6]
    assert smr.get_results('Broooklyn').to_list() == stub_match
