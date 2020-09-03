# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract class for openclean approximate string matching operators and class for String Matcher results"""

from abc import ABCMeta, abstractmethod
from typing import List, Tuple, Any, Dict, Union
from operator import itemgetter
import pandas as pd


class StringMatcherResults:
    """Class to maintain the string matcher results
        """

    class StringMatcherResultItem:
        """Class to maintain the string matcher algorithm results for a single search query
        """

        def __init__(self, query: str):
            """Initialize the StringMatcherResults class.

            Parameters
            ----------
            query : str
                the search query
            """
            self.matches = list() # will store all the tuples of (score, match)
            self.exact = False # True if a 100% match found with the search query
            self.query = query

        def __repr__(self):
            return str(self.matches)

        def __len__(self):
            return len(self.matches)

        def add(self, score: float, value: str):
            """
            Adds a tuple of (score, value) to the results list.

            Parameters:
            ----------
            score: float
                the score
            value: str
                the matched string
            """
            self.matches.append((score, value))

        def get(self, score_threshold: float = 0, fetch_score: bool = False) -> List:
            """
            Returns self.matches or just the values for any match greater than the threshold

            Parameters:
            ----------
            fetch_score: bool
                whether to include the scores with the returned matches
            score_threshold: float
                the score threshold to filter on
            Returns:
            _______
            list
            """
            matches = list()
            for m in self.matches:
                if m[0] > score_threshold:
                    if fetch_score:
                        matches.append(m)
                    else:
                        matches.append(m[1])
            return matches

        def is_empty(self) -> bool:
            """tests the object to check if there were no matches found"""
            return self.__len__() == 0

        def is_exact(self) -> bool:
            """tests the object to check if a 100% match was found"""
            return self.exact

        @staticmethod
        def get_top_matches(matches: List[Tuple[float, str]]) -> List:
            """
            Gets all the matches with the maximum score.

            Returns:
            --------
            list
            """
            matches = matches.copy()
            if len(matches) > 0:
                matches.sort(key=itemgetter(0), reverse=True)
                max_score = matches[0][0]
                for match in matches:
                    if match[0] < max_score:
                        matches.remove(match)
            return matches

        @classmethod
        def from_list(cls, matches: List[Tuple[float, str]]):
            """Factory method to create class from a list of matches

            Parameters
            ----------
                matches: list
                    The list of tuples (score, match) to crete a class object from
            """
            smri = cls.__new__(cls)
            for m in matches:
                smri.add(score=m[0], value=m[1])
            return smri

        def to_list(self):
            return self.matches

    def __init__(self):
        """Initialize a StringMatcherResults object"""
        self.results = dict()
        self.indices = dict()

    def add(self, index: Any, query: str, match: List[Tuple[float, str]]):
        """
        Add a StringMatcherResultItem to the StringMatcherResults object

        Parameters
        ----------
        index: Any
            the index of the input query
        query: str
            the search query
        match: list
            the list of tuples
        """
        filtered_matches = self.StringMatcherResultItem(query=query)
        for m in match:
            filtered_matches.add(score=m[0], value=m[1])
            if m[1] == query:
                filtered_matches.exact = True

        self.results[query] = filtered_matches
        self.append_index(query=query, index=index)

    def append_index(self, query: str, index: Any):
        """ Appends to the list of indices for the search query

        Parameters
        ----------
        query: str
            the search query
        index: Any
            the index of the input query
        """
        if query not in self.indices:
            self.indices[query] = list()
        self.indices[query].append(index)

    def get_indices(self, query: str = None) -> Union[List, Dict]:
        """ returns the index list for all or specific query

        Parameters
        ----------
        query: str
            the search query
        """
        return self.indices[query] if query is not None else self.indices

    def get_results(self, query: str = None) -> Union[List, Dict]:
        """ returns the results for all or specific query

        Parameters
        ----------
        query: str
            the search query
        """
        return self.results[query] if query is not None else self.results

    def get_mismatches(self):
        """ returns results that didnt yield a 100% match
        """
        return {query: match for query, match in self.results.items() if not match.is_exact()}


class StringMatcher(metaclass=ABCMeta):
    """Abstract class for string matching operators that take a list as input master data and a list of input test data
        and return a dict as output.
        """

    def __init__(self, masterdata: List):
        """Initialize the StringMatcher class with the masterdata.

        Parameters
        ----------
        masterdata: list
            Master data provided by the user.
        """
        self.masterdata = masterdata

    @abstractmethod
    def match(self, df: pd.Series) -> StringMatcherResults:
        """This is the main method that each subclass of the StringMatcher has to
        implement. The input is a list. The output is a list of tuples: [(score1, match1),(score2, match2)...].

        Parameters
        ----------
        df: pandas DataSeries
            Input dataseries

        Returns
        -------
        StringMatcherResults
        """
        raise NotImplementedError()
