# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Approximate String Matching using the fuzzyset library"""

from openclean.profiling.matcher.base import StringMatcher, StringMatcherResults
from fuzzyset import FuzzySet
import pandas as pd
from typing import List


class FuzzyStringMatcher(StringMatcher):
    """
    use (FuzzySet)[https://github.com/axiak/fuzzyset/] to do approximate string matching operations

    Note: it converts everything to lowercase and removes all punctuation except commas and spaces
    """

    def __init__(self, masterdata: List, **kwargs):
        """Initializes the fuzzy string matcher class with master data and any other parameters specific to the FuzzySey class
        """
        super(FuzzyStringMatcher, self).__init__(masterdata=masterdata)
        self.fuzzyset = FuzzySet(iterable=masterdata,
                                 gram_size_lower=kwargs.get('gram_size_lower', 2),
                                 gram_size_upper=kwargs.get('gram_size_upper', 3),
                                 use_levenshtein=kwargs.get('use_levenshtein', True),
                                 rel_sim_cutoff=kwargs.get('rel_sim_cutoff', 1))

    def match(self, df: pd.Series) -> StringMatcherResults:
        """Main match method to be implemented to lookup a single value from the masterdata and calculate a score

        Parameters
        ----------
        df: pandas Series
            the data series with input strings to lookup

        Returns
        -------
        list
        """
        results = StringMatcherResults()
        for index, query in df.items():
            match = self.fuzzyset.get(query)
            if match is None:
                match = []
            results.add(index=index, query=query, match=match)
        return results
