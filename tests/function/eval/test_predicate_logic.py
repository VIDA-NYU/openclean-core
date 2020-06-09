# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for predicate logic operators."""

from openclean.function.eval.column import Col
from openclean.function.eval.predicate.comp import Eq, Gt
from openclean.function.eval.predicate.logic import And, Not, Or


def test_predicate_logic(employees):
    """Test functionality of logic operators."""
    # -- And ------------------------------------------------------------------
    f = And(Gt(Col('Name'), 'Claudia'), Gt(Col('Age'), 30))
    f.prepare(employees)
    assert not f(employees.iloc[0])
    assert not f(employees.iloc[1])
    assert f(employees.iloc[5])
    # -- Or -------------------------------------------------------------------
    f = Or(Gt(Col('Name'), 'Claudia'), Gt(Col('Age'), 30))
    f.prepare(employees)
    assert not f(employees.iloc[0])
    assert f(employees.iloc[1])
    assert f(employees.iloc[5])
    # Not ---------------------------------------------------------------------
    f = Not(Eq(Col('Name'), 'Alice'))
    f.prepare(employees)
    assert not f(employees.iloc[0])
    assert f(employees.iloc[1])
