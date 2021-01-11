# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for the data repositories registry."""

from openclean.data.source.registry import repositories


def test_repository_listing():
    """Test the default downloader repository registry."""
    assert len(repositories()) == 3
