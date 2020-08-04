# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper and utility functions."""


def ensure_callable(func):
    """Helper method to test whether a given argument is a callable function
    or not. Raises a ValueError if the argument is not a callable.

    Parameters
    ----------
    func: any
        Object that is tested for being a callable.

    Returns
    -------
    callable

    Raises
    ------
    ValueError
    """
    if not callable(func):
        raise ValueError("'{}' not a callable".format(func))
    return func


def funcname(f):
    """Get the name of a function or class object.

    Parameters
    ----------
    f: callable
        Function or class object

    Returns
    -------
    string
    """
    try:
        return f.__name__
    except AttributeError:
        return f.__class__.__name__


def is_list_or_tuple(value):
    """Test if a given value is a list or tuple that can be converted into
    multiple arguments.

    Parameters
    ----------
    value: any
        Any object that is tested for being a list or tuple.

    Returns
    -------
    bool
    """
    return isinstance(value, list) or isinstance(value, tuple)
