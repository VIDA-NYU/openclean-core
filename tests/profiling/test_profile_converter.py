# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the datatype converter that is used for profiling."""

from dateutil.parser import parse

from openclean.profiling.datatype import DefaultConverter


def test_default_datatype_converter():
    """Test the default value converter."""
    converter = DefaultConverter()
    assert converter.convert('123') == (123, 'int')
    assert converter.convert('12.34') == (12.34, 'float')
    assert converter.convert('2020/05/15') == (parse('2020/05/15'), 'date')
    assert converter.convert('ABC') == ('ABC', 'text')
