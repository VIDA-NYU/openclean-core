# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.


class tautology(object):
    """A tautology is a predicate that is true in every interpretation. The
    tautology object is a callable that returns a pre-defined result (truth
    value) everytime it is invoked.
    """
    def __init__(self, return_value=True):
        """Initialize the value that is returned when the object is called.

        Parameters
        ----------
        return_value: scalar, optional
            Return value when the object is called.
        """
        self.return_value = return_value

    def __call__(self, value):
        """A call to the object returns the default value that was defined at
        object instantiation.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        scalar
        """
        return self.return_value
