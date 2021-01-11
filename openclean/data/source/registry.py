# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper function to get a list of known data repositories. By now this simply
hard-codes a list that contains handles all known repository implementations.
In the future we may want to maintain some form of database for these classes.
"""

from typing import List

from openclean.data.source.base import DataRepository
from openclean.data.source.britannica import EncyclopaediaBritannica
from openclean.data.source.restcountries import RestcountriesRepository
from openclean.data.source.socrata import Socrata


def repositories() -> List[DataRepository]:
    """Get list of all registered repositories

    Returns
    -------
    list of openclean.data.source.base.DataRepository
    """
    return [EncyclopaediaBritannica(), RestcountriesRepository(), Socrata()]
