# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Schema helper functions imported from HISTORE."""

# The signature of the imported functions are:
#
# as_list(columns: Columns) -> List[Union[int, str, Column]]
# column_ref(schema: Schema, column: ColumnRef) -> Tuple[str, int]
# select_clause(schema: Schema, columns: Columns) -> Tuple[List[str], List[int]]

from histore.document.schema import as_list, column_ref, select_clause, to_schema  # noqa: F401
