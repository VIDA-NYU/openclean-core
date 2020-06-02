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

from abc import ABCMeta, abstractmethod


class SequenceProfiler(metaclass=ABCMeta):
    """Profiler for a sequence of data values. Values are either scalar values
    or tuples of scalar values. Each sequence profiler implements the exec()
    method and the values() method. Both methods consume an iterable sequence
    of values. For some profilers it makes sense to implement either the exec
    method or the values method, depending on the type of profiling task that
    is being performed.

    The exec method returns a dictionary where the keys are values either from
    the input sequence or newly computed values (e.g., patterns). With each key
    one or more feature values may be associated (e.g., value frequency). The
    keys and values in the dictionary are implementation dependent and differ
    for each profiler. Note that the result does not have to contain all (or
    even any) of the original values.

    The values method simply returns the list of keys in the dictionary that
    the exec method returns. For profiling tasks that for example detect
    outliers, values() returns the list of detected outliers. The result of the
    exec method may contain additional weights or scores that represent the
    confidence or additional evidence for the returned values.
    """
    @abstractmethod
    def exec(self, values):
        """Compute one or more features over values in a given sequence.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        raise NotImplementedError()

    @abstractmethod
    def values(self, values):
        """Return a list values computed over the given input sequence. The
        list contains all values that are returned as keys in the disctionary
        from the exec method.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        raise NotImplementedError()


class FeatureProfiler(SequenceProfiler):
    """Sub-class for profilers that compute one or more features for a given
    sequence of values. Implements the values() method to simply return the
    list of keys in the dictionary that is returned by the exec() method.
    """
    def values(self, values):
        """Return a list values computed over the given input sequence. The
        list contains all values that are returned as keys in the disctionary
        from the exec method.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        return list(self.exec(values).keys())
