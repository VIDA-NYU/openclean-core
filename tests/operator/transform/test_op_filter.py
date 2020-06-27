# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for filter operator."""

from openclean.data.sequence import Sequence
from openclean.function.eval.base import Col
from openclean.function.value.domain import IsInDomain
from openclean.operator.transform.filter import delete, filter


def test_delete_operator(dupcols):
    """Test delete rows in a data frame."""
    assert 'Alice' in Sequence(dupcols, 'Name')
    df = delete(dupcols, Col('Name') == 'Alice')
    assert df.shape == (6, 3)
    assert 'Alice' not in Sequence(df, 'Name')


def test_filter_operator(agencies):
    """Test filtering rows in data frame."""
    # There are two rows with empty values in attribute Age
    d1 = filter(
        agencies,
        columns=['borough', 'state'],
        predicate=IsInDomain([('bk', 'Ny'), ('Bx', 'NY')], ignore_case=True)
    )
    assert d1.shape == (7, 3)
    d1 = filter(agencies, Col('agency') > Col('state'))
    assert d1.shape == (2, 3)
