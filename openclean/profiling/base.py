# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for operators that perform data profiling on a sequence
of data values.

Profilers can perform a wide range of tasks on a given sequence of values. Some
profiling operators compute one or more features for all values in the sequence
(e.g., frequency). Other examples of profilers detect outliers in a sequence of
values. That is, they filter values based on some condition computed over the
value features. Profilers can also compute new 'value', for example, when
discovering patterns in the data.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Optional, List

import pandas as pd

from openclean.data.column import Columns
from openclean.operator.collector.count import distinct

import openclean.util as util


# -- Column profiling operator ------------------------------------------------

def profile(
    df: pd.DataFrame, columns: Optional[Columns] = None,
    profilers: Optional[List[ProfilingFunction]] = None
):
    """Generic profiler that executes a list of associated profiling functions
    on a given list of values and combined their results in a dictionary.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    profilers: list(openclean.function.value.base.ProfilingFunction)
        List of profiling functions,
    """
    if columns is None:
        # If no columns are given we use the full data frame schema.
        columns = tuple(df.columns)
    # Compute set of distinct values with their frequency counts.
    values = distinct(df, columns=columns)
    return Profiler(profilers=profilers).run(values=values)


# --Profler base classes ------------------------------------------------------

class ProfilingFunction(metaclass=ABCMeta):
    """Profiler for a set of distinct values. Profiling functions compute
    statistics or informative summaries over a set of (distinct) values.

    Each profiler implements the exec_distinct() method. The method consumes a
    dictionary of distinct values mapped to their respective frequency counts.
    The result type of each profiler is implementation dependent. It should
    either be a scalar value (e.g. for aggregators) or a dictionary.

    Each profiling function has a (unique) name. The name is used as the key
    value in a dictionary that composes the results of multiple profiling
    functions.
    """
    def __init__(self, name=None):
        """Initialize the function name.

        Parameters
        ----------
        name: string, default=None
            Unique function name.
        """
        self.name = name if name else util.funcname(self)

    @abstractmethod
    def run(self, values):
        """Compute one or more features over a set of distinct values. This is
        the main profiling function that computes statistics or informative
        summaries over the given data values. It operates on a compact form of
        a value list that only contains the distinct values and their frequency
        counts.

        The return type of this function is implementation dependend.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        scalar value, list, set, or dict
        """
        raise NotImplementedError()


class Profiler(ProfilingFunction):
    """Generic profiler that executes a list of associated profiling functions
    on a given list of values and combined their results in a dictionary.
    """
    def __init__(self, profilers, name=None):
        """Initialize the list of profiler functions and the optional profiler
        name.

        Raises a ValueError if not all profilers are instances of the
        ProfilerFunction class or if the functon names are not unique.

        Parameters
        ----------
        profilers: list(openclean.function.value.base.ProfilingFunction)
            List of profiling functions,
        name: string, default=None
            Optional profiler name.

        Raises
        ------
        ValueError
        """
        # Ensure that profilers is a list of profiling functions with unique
        # names that are not empty.
        if not isinstance(profilers, list):
            profilers = [profilers]
        names = set()
        for f in profilers:
            if not isinstance(f, ProfilingFunction):
                raise ValueError("invalid profiler type '{}'".format(type(f)))
            fname = f.name
            if not fname:
                raise ValueError("invalid profiler name '{}'".format(fname))
            if fname in names:
                raise ValueError("duplicate profiler name '{}'".format(fname))
            names.add(fname)
        self.profilers = profilers
        super(Profiler, self).__init__(name=name if name else 'profiler')

    def run(self, values):
        """Combine the results from the different profiler functions in a
        single dictionary. Raises a ValueError if the names of the profilers
        are not unique.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        results = dict()
        for f in self.profilers:
            if f.name in results:
                raise ValueError('duplicate profiler name {}'.format(f.name()))
            results[f.name] = f.run(values)
        return results
