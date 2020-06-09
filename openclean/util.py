# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper and utility functions."""


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
