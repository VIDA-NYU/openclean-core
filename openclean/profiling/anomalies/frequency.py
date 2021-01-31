# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for frequency outlier detection."""

from collections import Counter
from typing import Callable, Dict, List, Optional, Union

import pandas as pd

from openclean.data.types import Value
from openclean.function.eval.base import InputColumn
from openclean.function.value.normalize import DivideByTotal, NumericNormalizer
from openclean.profiling.anomalies.base import AnomalyDetector
from openclean.util.threshold import to_threshold

ABSOLUTE = 'absolute'
NORMALIZED = 'normalized'


# -- Outlier results ----------------------------------------------------------

class FrequencyOutlierResults(list):
    """Frequency outlier results are a list of dictionaries. Each dictionary
    contains information about a detected outlier value ('value') and additional
    frequency metadata ('metadata': {'count', 'frequency'}).

    This class provides some basic functionality to access the individual
    pieces of information from these dictionaries.
    """
    def add(self, value: Value, count: int, frequency: Optional[float] = None):
        """Add a new outlier to the list.

        Parameters
        ----------
        value: scalar or tuple
            The outlier value.
        count: int
            Value frequency count.
        frequency: float, default=None
            Normalized value frequency (if a normalizer as used).
        """
        metadata = {'count': count}
        if frequency is not None:
            metadata['frequency'] = frequency
        self.append({'value': value, 'metadata': metadata})

    def counts(self) -> Counter:
        """Get a mapping of outlier values to their frequency counts.

        Returns
        -------
        collections.Counter
        """
        counter = Counter()
        for o in self:
            counter[o['value']] = o['metadata']['count']
        return counter

    def frequencies(self) -> Dict:
        """Get a mapping of outlier values to their normalized frequencies.

        Returns
        -------
        dict

        Raises
        ------
        KeyError
        """
        counter = dict()
        for o in self:
            counter[o['value']] = o['metadata']['frequency']
        return counter

    def values(self) -> List:
        """Get only the list of outlier vaues.

        Returns
        -------
        list
        """
        return [o['value'] for o in self]


# -- Frequency outlier detection operators ------------------------------------

def frequency_outliers(
    df: pd.DataFrame, columns: Union[InputColumn, List[InputColumn]],
    threshold: Union[Callable, int, float],
    normalize: Optional[NumericNormalizer] = DivideByTotal()
) -> FrequencyOutlierResults:
    """Detect frequency outliers for values (or value combinations) in one or
    more columns of a data frame. A value (combination) is considered an
    outlier if the relative frequency satisfies the given threshold predicate.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    threshold: callable
        Function that accepts a float (i.e., the relative frequency) and that
        returns a Boolean value. True indicates that the value (frequency)
        satisfies the value outlier condition.
    normalize: openclean.function.value.normalize.NumericNormalizer
        Function used to normalize frequency values befor evaluating the
        threshold constraint.

    Returns
    -------
    openclean.profiling.anomalies.frequency.FrequencyOutlierResults
    """
    return FrequencyOutliers(
        threshold=threshold,
        normalize=normalize
    ).run(df=df, columns=columns)


class FrequencyOutliers(AnomalyDetector):
    """Detect frequency outliers for values in a given list. A value is
    considered an outlier if its relative frequency in the list satisfies the
    given threshold predicate.
    """
    def __init__(
        self, threshold: Union[Callable, int, float],
        normalize: Optional[NumericNormalizer] = DivideByTotal()
    ):
        """Initialize the frequency threshold.

        Parameters
        ----------
        threshold: callable
            Function that accepts a float (i.e., the relative frequency) and
            that returns a Boolean value. True indicates that the value
            (frequency) satisfies the value outlier condition.
        normalize: openclean.function.value.normalize.NumericNormalizer
            Function used to normalize frequency values befor evaluating the
            threshold constraint.
        """
        self.threshold = to_threshold(threshold)
        self.normalize = normalize

    def process(self, values: Counter) -> FrequencyOutlierResults:
        """Normalize the frequency counts in the given mapping. Returns all
        values that satisfy the threshold constraint together with their
        normalized (and absolute) frequencies.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        openclean.profiling.anomalies.frequency.FrequencyOutlierResults
        """
        result = FrequencyOutlierResults()
        if self.normalize is not None:
            f = self.normalize.prepare(values.values())
            for value, count in values.items():
                freq = f.eval(count)
                if self.threshold(freq):
                    result.add(value=value, count=count, frequency=freq)
        else:
            for value, count in values.items():
                if self.threshold(count):
                    result.add(value=value, count=count)
        return result
