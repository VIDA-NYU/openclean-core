# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.data.types import Column
from openclean.data.select import select_clause


def test_select_clause_without_duplicates(employees):
    """Test the select clause method that extracts a list of column names and
    their index positions frm a data frame schema.
    """
    colnames, colidxs = select_clause(employees.columns, 'Name')
    assert colnames == ['Name']
    assert colidxs == [0]
    colnames, colidxs = select_clause(employees.columns, 'Age')
    assert colnames == ['Age']
    assert colidxs == [1]
    colnames, colidxs = select_clause(employees.columns, ['Salary', 'Age'])
    assert colnames == ['Salary', 'Age']
    assert colidxs == [2, 1]
    colnames, colidxs = select_clause(employees.columns, employees.columns)
    assert colnames == ['Name', 'Age', 'Salary']
    assert colidxs == [0, 1, 2]


def test_select_clause_with_duplicates(dupcols):
    """Test the select clause method for a data frame schema with duplicate
    column names.
    """
    # -- Use only the columns name (returns the first 'A' column) -------------
    columns = ['Name', 'A']
    colnames, colidxs = select_clause(dupcols.columns, columns)
    assert colnames == ['Name', 'A']
    assert colidxs == [0, 1]
    # -- Use column objects (returns the second 'A' column) -------------------
    columns = [dupcols.columns[0], dupcols.columns[2]]
    colnames, colidxs = select_clause(dupcols.columns, columns)
    assert colnames == ['Name', 'A']
    assert colidxs == [0, 2]
    # -- Error if the column index is out of range ----------------------------
    columns = [dupcols.columns[0], Column(colid=10, name='A', colidx=20)]
    with pytest.raises(ValueError):
        select_clause(dupcols.columns, columns)
    # -- Error if the column index and the name don't match -------------------
    columns = [dupcols.columns[0], Column(colid=10, name='B', colidx=2)]
    with pytest.raises(ValueError):
        select_clause(dupcols.columns, columns)
