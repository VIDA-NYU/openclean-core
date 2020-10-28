# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from collections import defaultdict, Counter
from typing import Iterable, List, Optional, Dict
import warnings

from openclean.function.matching.base import StringMatchResult, StringMatcher  # noqa: E501


class Mapping(defaultdict):
    """The mapping class is a lookup dictionary that is used to maintain a
    mapping of values (e.g., from a data frame columns) to their nearest match
    (or list of nearest matches) in a controlled vocabulary.
    """

    def __init__(self, values: Optional[Iterable[str]] = None):
        """Initialize the (optional) mapping of terms in a controlled vocabulary
        to themself.

        Parameters
        ----------
        values: iterator of string
            Iterable list of terms in a controlled vocabulary.
        """
        super(Mapping, self).__init__(list)
        if values is not None:
            for val in values:
                self[val].append((val, 1.))

    def add(
        self, key: str, matches: List[StringMatchResult]
    ) -> List[StringMatchResult]:
        """Add a list of matches to the mapped values for a given term (key).
        The term that is identified by the key does not have to exist in the
        current mapping.

        Returns the resulting list of matches for the given key.

        Parameters
        ----------
        key: string
            Term in the mapped vocabulary or that is added to the mapping.
        matches: list of StringMatchResult
            The list of matches returned from a matcher
        Returns
        -------
        list of matching results
        """
        for m in matches:
            self[key].append(m)
        return self[key]

    def update(self, updates: Optional[Dict[str, str]] = None) -> None:
        """lets the user update values in the map with their own values

        Parameters
        ----------
        updates: dict
            dictionary of type {mapping_key: best_match}

        Raises
        ------
        Key Error
        """
        updates = {} if updates is None else updates
        missing_keys = []
        [missing_keys.append(k) for k in updates if k not in self]
        if not len(missing_keys):
            for k, v in updates.items():
                super(Mapping, self).update({k: [(1., v)]})
        else:
            raise KeyError("Key(s) not found: {}".format(str(missing_keys)))

    def match_counts(self) -> Counter:
        """Counts the matches for each key in the map.

        Returns the resulting Counter.

        Returns
        -------
        Counter of # of matches
        """
        counter = Counter()
        for key, value in self.items():
            counter[key] = len(value)
        return counter

    def unmatched(self) -> set:
        """Identifies keys that have no matches

        Returns the resulting Set.

        Returns
        -------
        Set of keys with no matches
        """
        unmatched = set()
        for key, value in self.items():
            if len(value) == 0:
                unmatched.add(key)
        return unmatched

    def matched(self, single_match_only: bool = False) -> defaultdict:
        """Identifies keys with one or more than one matches in the map.

        Returns the resulting Mapping.

        Parameters
        ----------
        single_match_only: bool
            selects between keys with only one or atleast one matches

        Returns
        -------
        Mapping of matched results
        """
        matched = Mapping()
        for key, value in self.items():
            if single_match_only and len(value) == 1:
                matched.add(key=key, matches=value)
            elif not single_match_only and len(value) >= 1:
                matched.add(key=key, matches=value)
        return matched

    def filter(self, terms: Iterable[str]) -> defaultdict:
        """Get a mapping for only the terms in a given list

        Returns the resulting Mapping.

        Parameters
        ----------
        terms: Iterable of strings
            the list of keys to return from the mapper

        Returns
        -------
        Mapping of selected keys and their matches
        """
        filtered = Mapping()
        for term in terms:
            filtered.add(key=term, matches=self[term])
        return filtered

    def to_lookup(self, raise_exceptions: bool = False) -> dict:
        """Convert map into dict of key:match

        Note: incase of multiple matches, it'll ignore those keys and raise a warning

        Returns
        -------
        dict of keys and their matches

        Raises
        ------
        RuntimeError
        """
        values = dict()
        for k, v in self.items():
            if isinstance(v, list) and isinstance(v[0], tuple):
                if len(v) == 1:
                    values[k] = v[0][1]
                else:
                    if not raise_exceptions:
                        warnings.warn('Ignoring key: {} ({} matches). To include ignored keys, '
                                      'update the map to contain only 1 match per key'.format(k, len(v)))
                    else:
                        raise RuntimeError('Operation failed. Found duplicate matches for key: {}'.format(k))
            else:
                raise RuntimeError("Malformed values for key: {}".format(k))
        return values


def best_matches(
        values: Iterable[str],
        matcher: StringMatcher,
        include_vocab: Optional[bool] = False
) -> Mapping:
    """Generate a mapping of best matches for a list of values. For each value
    in the given list the best matches with a given vocabulary are computed and
    added to the returned mapping.

    If the include_vocab flag is False the resulting mapping will contain a
    mapping only for those values in the input list that do not already occur
    in the vocabulary, i.e., the unknown values with respect to the known
    vocabulary.

    Parameters
    ----------
    values: iterable of strings
        List of terms (e.g., from a data frame column) for which matches are
        computed for the returned mapping.
    matcher: openclean.function.matching.base.StringMatcher
        Matcher to compute matches for the terms in a controlled vocabulary.
    include_vocab: bool, default=False
        If this flag is False the resulting mapping will only contain matches
        for terms that are not in the vocabulary that is associated with the
        given similarity.

    Returns
    -------
    openclean.function.matching.mapping.Mapping
    """
    map = Mapping()
    for val in values:
        if include_vocab or val not in matcher.vocabulary:
            map.add(val, matcher.find_matches(val))
    return map
