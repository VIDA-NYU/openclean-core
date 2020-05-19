# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for the mapping generator."""

from openclean.data.transform import to_lookup
from openclean.function.replace import Lookup
from openclean.operator.transform.mapping import mapping


def test_country_mapping(countries, idi):
    """Create a mapping of country names in the IDI dataset to contry names in
    the ground truth data based on a mapping of contry identifier to contry
    names.
    """
    func = Lookup('country_id', to_lookup(countries, 'alpha3Code', 'name'))
    country_to_country = mapping(idi, 'country_name', func)
    assert country_to_country.shape == (167, 2)
