# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data management."""

from typing import Optional


class Descriptor(object):
    """Basic descriptor for a data object. Each object has a unique identifier
    and an optional human-readable name and a descriptive help text.
    """
    def __init__(
        self, identifier: str, name: Optional[str] = None,
        description: Optional[str] = None
    ):
        """Initialize the descriptor properties. If no name is given the
        identifier is used as the name.

        Parameters
        ----------
        identifier: string
            Unique object identifier.
        name: string, default=None
            Human-readable name.
        description: string, default=None
            Descriptive help text.
        """
        self.identifier = identifier
        self.name = name if name is not None else identifier
        self.description = description
