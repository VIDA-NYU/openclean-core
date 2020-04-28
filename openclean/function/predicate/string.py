# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of predicates that operate on string values from cells in data
frame rows.
"""

from openclean.function.base import SingleColumnEval

import openclean.function.value.string as vfunc


class Tokens(SingleColumnEval):
    """Predicate that evaluates the number of tokens returned by a split
    function for the given separator). This function operates on single columns
    only.
    """
    def __init__(
        self, columns, sep, validate, as_string=False, raise_error=False
    ):
        """Initialize the the source column, the separation delimiter, and the
        token length comparator. Raises a ValueError if more than one column is
        specified.

        Parameters
        ----------
        columns: int or string
            Single column index or name.
        sep: string, optional
            Delimiter string.
        validate: int or callable
            Compare predicate that is evaluated on the number of tokens
            returned by the split funciton.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        # Raise an error if more than one column is specified.
        if not type(columns) in [int, str]:
            raise ValueError('invalid column {}'.format(columns))
        super(Tokens, self).__init__(
            columns=columns,
            func=vfunc.tokens(
                sep=sep,
                validate=validate,
                as_string=as_string,
                raise_error=raise_error
            )
        )
