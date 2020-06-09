# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for filter operator."""

from openclean.data.sequence import Sequence
from openclean.function.eval.column import Col
from openclean.function.eval.predicate.comp import Eq, Gt
from openclean.function.value.comp import Gt as gt
from openclean.function.value.domain import IsInDomain
from openclean.operator.transform.filter import delete, filter


def test_delete_operator(dupcols):
    """Test delete rows in a data frame."""
    # There are two rows with age values that are greater than 35. One of them
    # is Dave.
    assert 'Dave' in Sequence(dupcols, 'Name')
    # Test with value function and column argument.
    d1 = delete(dupcols, 1, gt(35))
    assert d1.shape == (4, 3)
    # Dave is not in the resulting data frame.
    assert 'Dave' not in Sequence(d1, 'Name')
    # Test with EvalFunction.
    assert 'Alice' in Sequence(d1, 'Name')
    d2 = delete(d1, Eq(Col('Name'), 'Alice'))
    assert d2.shape == (3, 3)
    assert 'Alice' not in Sequence(d2, 'Name')


def test_filter_operator(agencies):
    """Test filtering rows in data frame."""
    # There are two rows with empty values in attribute Age
    d1 = filter(
        agencies,
        columns=['borough', 'state'],
        predicate=IsInDomain([('bk', 'Ny'), ('Bx', 'NY')], ignore_case=True)
    )
    assert d1.shape == (7, 3)
    d1 = filter(agencies, predicate=Gt(Col('agency'), Col('state')))
    assert d1.shape == (2, 3)
