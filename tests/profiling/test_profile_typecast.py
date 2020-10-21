# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.profiling.datatype.convert import DefaultConverter
from openclean.profiling.datatype.operator import TypecastOperator


@pytest.mark.parametrize('converter', [None, DefaultConverter()])
def test_row_typecast(converter):
    """Test type casting values in a data stream row using a type converter."""
    op = TypecastOperator(converter=converter).open(None, [])
    assert op.handle(0, [1, '2', '3.5', 'A']) == [1, 2, 3.5, 'A']