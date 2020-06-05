# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data profiling function that generates counts for distinct values in a given
sequence of values.
"""

from collections import Counter

from openclean.data.sequence import Sequence
from openclean.function.base import DictionaryFunction
from openclean.function.list.helper import normalize


def distinct(
    df, columns=None, normalizer=None, keep_original=False, labels=None
):
    """Compute the set of distinct value combinations for a given list of
    columns. Returns a dictionary containing the distinct values (tuples)
    together with their frequency counts.

    If the optional normalization function is given, the frequency counts in
    the returned dictionary will be normalized. If the keep original flag is
    True, the returned dictionary will map key values to nested dictionaries
    that contain the original and the normalized value.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int or string or list(int or string), optional
        List of column index or column name for columns for which distinct
        value combinations are computed.
    normalizer: callable or openclean.function.base.ValueFunction,
            default=None
        Optional normalization function that will be used to normalize the
        frequency counts in the returned dictionary.
    keep_original: bool, default=False
        If the keep original value is set to True, the resulting dictionary
        will map key values to dictionaries. Each nested dictionary will
        have two elements, the original ('absolute') value and the
        normalized value.
    labels: list or tuple, default=('absolute', 'normalized')
        List or tuple with exactly two elements. The labels will only be
        used if the keep_original flag is True. The first element is the
        label for the original value in the returned nested dictionary and
        the second element is the label for the normalized value.

    Returns
    -------
    dict
    """
    op = Distinct(
        normalizer=normalizer,
        keep_original=keep_original,
        labels=labels
    )
    return op.map(Sequence(df=df, columns=columns))


class Distinct(DictionaryFunction):
    """Compute the set of distinct values in a given sequence together with
    their value frequency counts.
    """
    def __init__(
        self, normalizer=None, keep_original=False, labels=None, name=None
    ):
        """Initialize the optional normalizer. By default absolute frequency
        counts are returned for the distinct values.

        If the optional normalization function is given, the frequency counts
        in the returned dictionary will be normalized. If the keep original
        flag is True, the returned dictionary will map key values to nested
        dictionaries that contain the original and the normalized value.

        Parameters
        ----------
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize the
            frequency counts in the returned dictionary.
        keep_original: bool, default=False
            If the keep original value is set to True, the resulting dictionary
            will map key values to dictionaries. Each nested dictionary will
            have two elements, the original ('absolute') value and the
            normalized value.
        labels: list or tuple, default=('absolute', 'normalized')
            List or tuple with exactly two elements. The labels will only be
            used if the keep_original flag is True. The first element is the
            label for the original value in the returned nested dictionary and
            the second element is the label for the normalized value.
        name: string, default=None
            Unique function name.
        """
        self.normalizer = normalizer
        self.keep_original = keep_original
        self.labels = labels
        self._name = name

    def map(self, values):
        """Compute distinct values and their frequency counts for elements in
        the given sequence of scalar values or tuples of scalar values.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        # The collection.Counter class is a dictionary. It will automatically
        # create counts for the values in the iterator.
        counts = Counter(values)
        # Normalize the result if a normalizer is given.
        if self.normalizer is not None:
            counts = normalize(
                values=counts,
                normalizer=self.normalizer,
                keep_original=self.keep_original,
                labels=self.labels
            )
        return counts

    def name(self):
        """Get the unique function name.

        Returns
        -------
        string
        """
        if not self._name:
            name = 'distinctValueCount'
            if self.normalizer:
                name += 'Normalized'
            return name
        return self._name
