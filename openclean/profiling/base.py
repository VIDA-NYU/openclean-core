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

from openclean.data.sequence import Sequence
from openclean.function.base import ProfilingFunction


# -- Column profiling operator ------------------------------------------------

def profile(df, columns=None, profilers=None):
    """Generic profiler that executes a list of associated profiling functions
    on a given list of values and combined their results in a dictionary.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) for which profiles are computed.
    profilers: list(openclean.function.base.ProfilingFunction)
        List of profiling functions,
    """
    values = Sequence(df=df, columns=columns)
    return Profiler(profilers=profilers).exec(values=values)


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
        profilers: list(openclean.function.base.ProfilingFunction)
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
            fname = f.name()
            if not fname:
                raise ValueError("invalid profiler name '{}'".format(fname))
            if fname in names:
                raise ValueError("duplicate profiler name '{}'".format(fname))
            names.add(fname)
        self.profilers = profilers
        super(Profiler, self).__init__(name=name if name else 'profiler')

    def exec(self, values):
        """Combine the results from the different profiler functions in a
        single dictionary. Raises a ValueError if the names of the profilers
        are not unique.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        results = dict()
        for f in self.profilers:
            if f.name() in results:
                raise ValueError('duplicate profiler name {}'.format(f.name()))
            results[f.name()] = f.exec(values)
        return results
