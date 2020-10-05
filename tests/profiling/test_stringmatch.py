# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the approximate string matching operations"""

from openclean.profiling.stringmatch import StringMatch

def test_find_mismatches(zipcodes):
    """" Test find_mismatches works correctly """
    masterdata = ['Brooklyn','Manhattan','Queens','Staten Island','Bronx']
    smr = StringMatch(matcher='FUZZY', data=masterdata, on='Borough')
    mismatches = smr.find_mismatches(zipcodes)
    assert len(mismatches) == 2 and 'Broooklyn' in mismatches and 'Quees' in mismatches


def test_get_mismatched_rows(zipcodes):
    """" Test get_mismatched_rows works correctly """
    masterdata = ['Brooklyn','Manhattan','Queens','Staten Island','Bronx']
    smr = StringMatch(matcher='FUZZY', data=masterdata, on='Borough')
    mismatched_df = smr.get_mismatched_rows(zipcodes)
    assert mismatched_df.shape[0] == 2 and list(mismatched_df.index.values) == [4, 6]


def test_fix(zipcodes):
    """" Test fix works correctly """
    masterdata = ['Brooklyn', 'Manhattan', 'Queens', 'Staten Island', 'Bronx']
    smr = StringMatch(matcher='FUZZY', data=masterdata, on='Borough')
    fixed = smr.fix(df=zipcodes, score_threshold=0.5)
    assert smr.find_mismatches(fixed) == {}

    misspelled_inputs = smr.find_mismatches(zipcodes).keys()
    for mi in misspelled_inputs:
        assert not fixed['Borough'].str.contains(mi).any()

    bad_replacements = {'Brooklyn':'Brook','Manhattan':'Manhatta','Queens':'Freddie'}
    replaced = smr.fix(df=zipcodes, replacements=bad_replacements)
    replaced_mismatches = smr.find_mismatches(replaced)
    assert len(replaced_mismatches) == 5
    for mm in ['Brook','Manhatta','Freddie','Broooklyn','Quees']:
        assert mm in replaced_mismatches
