# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class to perform approximate string matching operations on a pandas dataframe."""
from openclean.profiling.matcher.base import StringMatcherResults
from openclean.profiling.matcher.fuzzy import FuzzyStringMatcher
import pandas as pd
from typing import Dict, List

COL_MATCHES = 'matches'
STR_FUZZY = 'FUZZY'
STRING_MATCHERS = [STR_FUZZY]


class StringMatch(object):
    """StringMatch class that takes in the master dataset and performs approximate string matching"""
    def __init__(self, data: List,
                 on: str,
                 matcher: str = 'FUZZY'):
        """Initialize the column names and the master data.

        Parameters
        ----------
        data: list
            The data to match with
        on: string
            The column to find matches in
        matcher: string
            The approximation algorithm to use
        """
        self.on = on
        if matcher not in STRING_MATCHERS:
            raise ValueError('string matcher not defined')
        elif matcher == STR_FUZZY:
            self.matcher = FuzzyStringMatcher(masterdata=data)

    def _find(self, df: pd.DataFrame) -> StringMatcherResults:
        """Gets all matches in the master data for  a pandas DataFrame column and returns a StringMatcherResults object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using the string matching algorithms

        Returns
        -------
        StringMatcherResults
        """
        return self.matcher.match(df[self.on])

    def find_mismatches(self, df: pd.DataFrame) -> Dict:
        """Identifies mismatches and closest matches in a pandas DataFrame column and returns a dict.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using the string matching algorithms
        score_threshold: float
            Threshold for score

        Returns
        -------
        dict: {query: list}
        """
        return self._find(df=df).get_mismatches()

    def get_mismatched_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Returns a dataframe of all rows that had items that didn't match with the master data.

        Parameters
        ----------
        df: pandas.DataFrame
            The input dataframe

        Returns
        -------
        pandas.DataFrame
        """
        mismatches = self.find_mismatches(df).keys()
        indices = self._find(df=df).get_indices()
        rows = list()
        for mismatch in mismatches:
            rows += indices[mismatch]
        return df.iloc[rows]

    def fix(self, df: pd.DataFrame, score_threshold: float = 0.8, replacements: Dict = None) -> pd.DataFrame:
        """updates the pandas.DataFrame object. If no replacements are provided, the replaced values are the highest
        scorers of the find_mismatches(df) dictionary. Incase there are two equally probable/scored replacements, raises
        an Exception

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to find mismatches in
        replacements: dict
            Dictionary of what to replace each value with: {value:replacement}
        score_threshold: float
            The threshold for scores for the matches to be considered replaceable
        Returns
        _______
        pandas.DataFrame
        """
        if replacements is None:
            replacements = dict()
            mismatches = self.find_mismatches(df=df)
            for query, matches in mismatches.items():
                filtered_matches = StringMatcherResults.StringMatcherResultItem.get_top_matches(matches.to_list())
                if len(filtered_matches) > 1:
                    raise ValueError("Multiple replacements found for {}: {}. Manually replace this value first.".format(query, matches.to_list()))

                if len(filtered_matches) == 1 and filtered_matches[0][0] > score_threshold:
                    replacements[query] = filtered_matches[0][1]

        return df.replace({self.on: replacements})
