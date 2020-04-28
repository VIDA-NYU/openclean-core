from openclean.data.column import select_clause


def test_select_clause(employees):
    colnames, colidxs = select_clause(employees, 'Name')
    assert colnames == ['Name']
    assert colidxs == [0]
    colnames, colidxs = select_clause(employees, 'Age')
    assert colnames == ['Age']
    assert colidxs == [1]
    colnames, colidxs = select_clause(employees, ['Salary', 'Age'])
    assert colnames == ['Salary', 'Age']
    assert colidxs == [2, 1]
    colnames, colidxs = select_clause(employees, employees.columns)
    assert colnames == ['Name', 'Age', 'Salary']
    assert colidxs == [0, 1, 2]
