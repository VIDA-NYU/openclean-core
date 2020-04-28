# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of predicates that operate on the characters in given scalar
values.
"""


class first_char_in_range(object):
    """Predicate that tests if the first character of a given scalar value is
    within a given range of characters. Start end end characters in the given
    range are inclusive.

    For values that are not of type string the default is to use their string
    representation. The user has the option to override this behavior by
    setting the as_string flag to False. In this case, the predicate raise a
    ValueError for non-string arguments.

    For empty strings and None's the default return value is False. If the user
    sets the ignore empty_flag to False a ValueError is raised instead for
    empty values.
    """
    def __init__(self, cstart, cend, as_string=True, ignore_empty=True):
        """Initialize the object properties. Raises a ValueError if an invalid
        interval is defined by the givven start end end characters.

        Parameters
        ----------
        cstart: char
            First character in the interval.
        cend: char
            Last character in the interval.
        as_string: bool, optional
            Use string representation for non-string values.
        ignore_empty: bool, optional
            Return False for empty values. If False, a ValueError is raised for
            empty values.

        Raises
        ------
        ValueError
        """
        # Ensure that the given character range defines a valid interval.
        try:
            if ord(cend) < ord(cstart):
                msg = 'invalid character range {}-{}'
                raise ValueError(msg.format(cstart, cend))
        except TypeError as ex:
            raise ValueError(ex)
        self.cstart = ord(cstart)
        self.cend = ord(cend)
        self.as_string = as_string
        self.ignore_empty = ignore_empty

    def __call__(self, value):
        """Test if the first character in the (string representation of the)
        given value falls withing the defined character interval.

        Raises a ValueError if the value is empty and the ignore_empty flag is
        False, or if the value is not a string and the as_string flag is False.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        # Handle None values
        if value is None:
            if self.ignore_empty:
                return False
            else:
                raise ValueError('empty value')
        # Raise error for non-string values if the as_string flag is False
        if not self.as_string and not type(value, str):
            raise ValueError('invalid argument {}'.format(value))
        # Test first character in the string representation of the given value.
        # Catch IndexError in case the string is empty
        try:
            return self.cstart <= ord(str(value)[0]) <= self.cend
        except IndexError:
            raise ValueError('empty value')
