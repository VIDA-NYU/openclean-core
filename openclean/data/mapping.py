# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from __future__ import annotations
from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Union

import warnings


# -- String match results -----------------------------------------------------

@dataclass
class StringMatch:
    """String matching results that contains te matched term an the match
    score. A score of 1. indicates a perfect match and a score of 0. a no-match.
    """
    # Matched terms.
    term: str
    # Match score (between 0-1).
    score: float


class ExactMatch(StringMatch):
    """Short cut for creating an exact string match result."""
    def __init__(self, term: str):
        """Initialize the matched term and the match score in the superclass.

        Parameters
        ----------
        term: string
            Matched term.
        """
        super(ExactMatch, self).__init__(term=term, score=1.)


class NoMatch(StringMatch):
    """Short cut for creating a no-match result for string matcher."""
    def __init__(self, term: str):
        """Initialize the matched term and the match score in the superclass.

        Parameters
        ----------
        term: string
            Matched term.
        """
        super(NoMatch, self).__init__(term=term, score=0.)


# -- String mappings ----------------------------------------------------------

class Mapping(defaultdict):
    """The mapping class is a lookup dictionary that is used to maintain a
    mapping of values (e.g., from a data frame columns) to their nearest match
    (or list of nearest matches) in a controlled vocabulary.
    """
    def __init__(self, values: Optional[Dict[str: Union[str, List[StringMatch]]]] = None):
        """Initialize the mapping of terms.

        Parameters
        ----------
        values: dict
            Mapping of terms to strings or string matches.
        """
        super(Mapping, self).__init__(list)
        if values is not None:
            for key, values in values.items():
                if isinstance(values, list):
                    self[key] = values
                else:
                    self[key] = [ExactMatch(values)]

    def add(self, key: str, matches: List[StringMatch]) -> Mapping:
        """Add a list of matches to the mapped values for a given term (key).
        The term that is identified by the key does not have to exist in the
        current mapping.

        Returns a reference to the mapping itself.

        Parameters
        ----------
        key: string
            Term in the mapped vocabulary or that is added to the mapping.
        matches: list of openclean.data.mapping.StringMatch
            The list of matches returned from a matcher

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        self[key].extend(matches)
        return self

    def filter(self, terms: Iterable[str]) -> Mapping:
        """Get a mapping for only the terms in a given list

        Returns the resulting Mapping.

        Parameters
        ----------
        terms: Iterable of strings
            the list of keys to return from the mapper

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        filtered = Mapping()
        for term in terms:
            filtered.add(key=term, matches=self[term])
        return filtered

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

    def matched(self, single_match_only: Optional[bool] = False) -> Mapping:
        """Identifies keys with one or more than one matches in the map.

        Returns the resulting Mapping.

        Parameters
        ----------
        single_match_only: bool
            selects between keys with only one or at least one matches

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        matched = Mapping()
        for key, value in self.items():
            if single_match_only and len(value) == 1:
                matched.add(key=key, matches=value)
            elif not single_match_only and len(value) >= 1:
                matched.add(key=key, matches=value)
        return matched

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

    def update(self, updates: Optional[Dict[str, str]] = None) -> Mapping:
        """Lets the user update values in the map with their own values. Raises
        an error if the provided dictionary contains keys that are not in the
        current mapping.

        The updated values are treated as exact matches (i.e., with a score of
        1.) since they are provided by the user.

        Returns a self reference.

        Parameters
        ----------
        updates: dict
            Dictionary of type {mapping_key: exact_match}

        Returns
        -------
        openclean.data.mapping.Mapping

        Raises
        ------
        Key Error
        """
        updates = {} if updates is None else updates
        # Ensure that there are no unknown keys in the update (i.e., keys that
        # are currently not included in the mapping).
        missing_keys = [k for k in updates if k not in self]
        if missing_keys:
            raise KeyError("Key(s) not found: {}".format(str(missing_keys)))
        for k, v in updates.items():
            super(Mapping, self).update({k: [ExactMatch(v)]})
        return self

    def to_lookup(self, raise_error: bool = False) -> Dict[str, str]:
        """Convert map into dict of key:match pairs.

        Note: in case of multiple matches, it'll ignore those keys and raise a
        warning.

        Returns
        -------
        dict of keys and their matches

        Raises
        ------
        RuntimeError
        """
        values = dict()
        for k, v in self.items():
            if len(v) == 1:
                values[k] = v[0].term
            else:
                if not raise_error:
                    warnings.warn('Ignoring key: {} ({} matches). To include ignored keys, '
                                  'update the map to contain only 1 match per key'.format(k, len(v)))
                else:
                    raise RuntimeError('Operation failed. Found duplicate matches for key: {}'.format(k))
        return values
