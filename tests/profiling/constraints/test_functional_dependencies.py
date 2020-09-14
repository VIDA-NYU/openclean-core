# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the functional dependency base class."""

from openclean.profiling.constraints.fd import FunctionalDependency
from openclean.profiling.constraints.ucc import UniqueColumnSet


def test_init_functional_dependency():
    """Test initialization of functional dependency objects."""
    fd = FunctionalDependency(
        lhs=UniqueColumnSet(columns=['A', 'B']),
        rhs=UniqueColumnSet(columns=['C', 'D'])
    )
    assert fd.lhs == {'A', 'B'}
    assert fd.rhs == {'C', 'D'}
    assert fd.lhs == fd.determinant
    assert fd.rhs == fd.dependant
